package MR::Tarantool::Box::Record::Object;

use Mouse;
use MR::Tarantool::Box::XS;
use overload
    '%{}' => sub { confess "Direct access to attributes through hash is provibited. Use accessors instead." },
    fallback => 1;

has shard_num => (
    is  => 'ro',
    isa => 'Int',
);

has replica => (
    is  => 'ro',
    isa => 'Bool',
    writer => '_replica',
);

has readonly => (
    is  => 'ro',
    isa => 'Bool',
    writer  => '_replica',
    lazy    => 1,
    default => sub { $_[0]->replica },
);

has exists => (
    is  => 'ro',
    isa => 'Bool',
    lazy    => 1,
    default => 0,
);

has _update_ops => (
    is  => 'ro',
    isa => 'ArrayRef',
    lazy    => 1,
    default => sub { [] },
);

sub select {
    shift->meta->select->(@_);
}

sub insert {
    my ($class, $list, %opts) = ref $_[0] ? (ref $_[0], [ shift ], @_) : @_;
    my @request;
    my $meta = $class->meta;
    my $insert_box = $meta->insert_box;
    foreach my $item (@$list) {
        confess "Can't insert readonly data" if $item->readonly;
        @{$item->_update_ops} = ();
        my %data = map { my $name = $_->name; $name => $item->$name } $meta->get_all_fields();
        if (my $serialize = $meta->serialize) {
            $serialize->([\%data]);
        }
        my $shard_num = $item->shard_num;
        if (my $validate = $meta->validate) {
            $validate->(\%data, $shard_num);
        }
        push @request, {
            %opts,
            type  => $insert_box ? 'call' : 'insert',
            tuple => \%data,
            from  => 'master',
            $shard_num ? (shard_num => $shard_num) : (),
        };
    }
    my $box = $insert_box || $meta->box;
    my $response = $box->bulk(\@request);
    my @failures = map {
        my $tuple = $request[$_]->{tuple};
        my $data = join ', ', map sprintf("%s: %s", $_->name, $_->value_for_debug($tuple->{$_->name})), $meta->get_all_fields();
        "$response->[$_]->{error}: [ $data ]";
    } grep { $response->[$_]->{error} != MR::Tarantool::Box::XS::ERR_CODE_OK } (0 .. $#$response);
    confess "Failed to insert $class: " . join(", ", @failures) if @failures;
    return;
}

sub update {
    my ($class, $list, %opts) = ref $_[0] ? (ref $_[0], [ shift ], @_) : @_;
    my @request;
    my $meta = $class->meta;
    my $serialize = $meta->serialize;
    my $primary_key = $meta->primary_key;
    my $prepare_key = $primary_key->prepare_key;
    my @itemops;
    foreach my $item (@$list) {
        my $ops = $item->_update_ops;
        next unless @$ops;
        confess "Can't update readonly data" if $item->readonly;
        push @itemops, $ops;
        if ($serialize) {
            $ops = [ @$ops ];
            foreach my $op (@$ops) {
                my $attr = $meta->get_attribute($op->[0]);
                if (my $serialize = $attr->serialize) {
                    local $_ = $op->[2];
                    $op->[2] = $serialize->($_);
                }
            }
        }
        my $key = $prepare_key->($item);
        my $shard_num = $item->shard_num;
        push @request, {
            %opts,
            type => 'update',
            key  => $key,
            ops  => $ops,
            $shard_num ? (shard_num => $shard_num) : (),
        };
    }
    my $response = $meta->box->bulk(\@request);
    my @failures;
    foreach (0 .. $#$response) {
        if ($response->[$_]->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK) {
            @{$itemops[$_]} = ();
        } else {
            push @failures, sprintf "%s [ %s ]", $response->[$_]->{error}, $primary_key->key_for_debug($request[$_]->{key}),
                join(", ", map sprintf("%s %s %s", $_->[0], $_->[1], $meta->get_attribute($_->[0])->value_for_debug($_->[2])), @{$request[$_]->{ops}});
        }
    }
    confess "Failed to update $class: " . join(", ", @failures) if @failures;
    return;
}

sub delete {
    my ($class, $list, %opts) = ref $_[0] ? (ref $_[0], [ shift ], @_) : @_;
    my @request;
    my $meta = $class->meta;
    my $delete_box = $meta->delete_box;
    my $primary_key = $meta->primary_key;
    my $prepare_key = $primary_key->prepare_key;
    foreach my $item (@$list) {
        confess "Can't delete readonly data" if $item->readonly;
        @{$item->_update_ops} = ();
        my $key = $prepare_key->($item);
        my $shard_num = $item->shard_num;
        push @request, {
            %opts,
            from  => 'master',
            $delete_box ? (type => 'call', tuple => $key) : (type => 'delete', key => $key),
            $shard_num ? (shard_num => $shard_num) : (),
        };
    }
    my $box = $delete_box || $meta->box;
    my $response = $box->bulk(\@request);
    my @failures = map sprintf("%s %s", $response->[$_]->{error}, $primary_key->key_for_debug($delete_box ? $request[$_]->{tuple} : $request[$_]->{key})),
        grep { $response->[$_]->{error} != MR::Tarantool::Box::XS::ERR_CODE_OK } (0 .. $#$response);
    confess "Failed to delete $class: " . join(", ", @failures) if @failures;
    return;
}

sub get_dirty_fields {
    my ($class, $list) = ref $_[0] ? (ref $_[0], [ shift ]) : @_;
    my %changed_fields = map { $_->[0] => $_->[2] } @{$list->[0]->_update_ops};
    return \%changed_fields;
}

sub _create_default {
    my ($class, $key, $value) = @_;
    return { $key => $value };
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
