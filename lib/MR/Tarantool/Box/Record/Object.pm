package MR::Tarantool::Box::Record::Object;

use Mouse;
use Carp 'cluck';
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
    default => 0,
);

has _update_ops => (
    is  => 'ro',
    isa => 'ArrayRef',
    default => sub { [] },
);

# dirty hack for trigger in old Mouse
has _built => (
    is  => 'rw',
);

sub BUILD {
    my ($self) = @_;
    $self->_init_new() unless $self->exists;
    $self->_built(1);
    return;
}

sub _init_new {}

sub select {
    my ($class, $field, $keys, %opts) = @_;
    my $keys_is_array = ref $keys eq 'ARRAY' || blessed $keys && overload::Method($keys, '@{}');
    my $noraise = delete $opts{noraise_unavailable};
    my $meta = $class->meta;
    my $box = $meta->box;
    my $index = $meta->index_by_name->{$field}
        or confess "Can't use field '$field' as an indexed field";
    my $uniq = $index->uniq;
    my $multifield = $index->multifield;
    my $single_multifield = $keys_is_array && $multifield && @$keys && ref $keys->[0] ne 'ARRAY';
    my $keys_is_bulk = $keys_is_array && !$single_multifield || ref $keys eq 'HASH';
    my $wantarrayref = $keys_is_bulk || !$uniq;
    $keys = [ $keys ] unless $keys_is_bulk;
    my $object_map = delete $opts{objects} || {};
    if (delete $opts{by_object} && (my $deobject_keys = $index->deobject_keys)) {
        $keys = $deobject_keys->($keys, $object_map);
    }
    my $shard_keys;
    my $prepare_keys = $index->prepare_keys;
    if (ref $keys eq 'HASH') {
        confess "option 'shard_num' shouldn't be used with \$keys passed as a HASHREF" if exists $opts{shard_num};
        $shard_keys = { map { $_ => $prepare_keys ? $prepare_keys->($keys->{$_}) : $keys->{$_} } keys %$keys };
    } else {
        my $shard_nums;
        if (!exists $opts{shard_num} && (my $shard_by = $index->shard_by)) {
            my $shard_num = $class->$shard_by($keys, objects => $object_map);
            if (ref $shard_num) {
                $shard_nums = $shard_num;
            } else {
                $opts{shard_num} = $shard_num;
            }
        }
        $keys = $prepare_keys->($keys) if $prepare_keys;
        if ($shard_nums) {
            confess "size of ARRAYREF returned by 'shard_by' function should be equal to size of keys ARRAYREF" unless @$shard_nums == @$keys;
            $shard_keys = {};
            foreach (0 .. $#$keys) {
                my $shard_num = $shard_nums->[$_] or next;
                push @{$shard_keys->{$shard_num}}, $keys->[$_];
            }
        } else {
            if ($opts{shard_num} && $opts{shard_num} eq 'all') {
                delete $opts{shard_num};
                $shard_keys = { map { $_ => $keys } (1 .. $box->iproto->get_shard_count()) };
            }
        }
    }
    my $create = delete $opts{create};
    confess "option 'create => 1' should be used only with unique singlefield indexes" if $create && (!$uniq || $multifield);
    $opts{limit} = $index->default_limit if !exists $opts{limit} && $index->has_default_limit();
    confess "option 'limit' should be specified or 'default_limit' should be set on index if non-unique index is used" if !$uniq && !exists $opts{limit};
    my $created;
    $opts{type} = 'select';
    $opts{use_index} = $index->index;
    $opts{inplace} = 1;
    my @request = $shard_keys ? map +{ %opts, keys => $shard_keys->{$_}, shard_num => $_ }, keys %$shard_keys : { %opts, keys => $keys };
    my $response = $box->bulk(\@request);
    my @alltuples;
    foreach my $resp (@$response) {
        if ($resp->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK) {
            my $tuples = $resp->{tuples};
            foreach my $tuple (@$tuples) {
                $tuple->{shard_num} = $resp->{shard_num} if exists $resp->{shard_num};
                $tuple->{replica} = 1 if $resp->{replica};
                $tuple->{exists} = 1;
            }
            if ($create && !$resp->{replica}) {
                my $key_field = $index->fields->[0];
                my %found = map { $_->{$key_field} => 1 } @$tuples;
                my @created = map $class->_create_default($key_field => $_), grep !$found{$_}, @{$resp->{keys}};
                push @alltuples, @created;
                $created = 1 if @created;
            }
            push @alltuples, @$tuples;
        } else {
            if ($noraise) {
                my $count = @{$resp->{keys}};
                cluck "Failed to select $class, $count items are unavailable: $resp->{error}";
            } else {
                confess "Failed to select $class: $resp->{error}";
            }
        }
    }
    if (my $limit = $opts{limit}) {
        $#alltuples = $limit - 1 if @alltuples > $limit;
    }
    if (my $deserialize = $meta->deserialize) {
        $deserialize->(\@alltuples);
    }
    if (%$object_map && (my $object_tuples = $meta->object_tuples)) {
        $object_tuples->(\@alltuples, $object_map);
    }
    my @list = map $class->new($_), @alltuples;
    if ($created) {
        $_->insert() foreach grep !$_->exists, @list;
    }
    if ($wantarrayref) {
        return \@list;
    } else {
        cluck sprintf "Select returned %d rows when only one row was expected", scalar @list if @list > 1;
        return $list[0];
    }
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
        push @request, {
            %opts,
            type  => $insert_box ? 'call' : 'insert',
            tuple => \%data,
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
            $delete_box ? (type => 'call', tuple => [ $key ]) : (type => 'delete', key => $key),
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

sub _create_default {
    my ($class, $key, $value) = @_;
    return { $key => $value };
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
