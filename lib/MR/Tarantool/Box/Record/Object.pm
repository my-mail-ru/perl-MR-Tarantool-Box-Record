package MR::Tarantool::Box::Record::Object;

use Mouse;
use Carp 'cluck';
use MR::Tarantool::Box::XS;

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
    my $noraise = delete $opts{noraise_unavailable};
    my $autocreate = delete $opts{autocreate};
    my $meta = $class->meta;
    my $box = $meta->box;
    my $index = $meta->index_by_name->{$field}
        or confess "Can't use field '$field' as an indexed field";
    my $single_multifield = ref $keys eq 'ARRAY' && $index->multifield && @$keys && !ref $keys->[0];
    my $wantarrayref = ref $keys && !$single_multifield || !$index->uniq;
    $keys = [ $keys ] if $single_multifield;
    my $shard_keys;
    my $prepare_keys = $index->prepare_keys;
    if (ref $keys eq 'HASH') {
        confess "option 'shard_num' shouldn't be used with \$keys passed as a HASHREF" if exists $opts{shard_num};
        $shard_keys = { map { $_ => $prepare_keys ? $prepare_keys->($keys->{$_}) : $keys->{$_} } keys %$keys };
    } else {
        $keys = [ $keys ] unless ref $keys;
        $keys = $prepare_keys->($keys) if $prepare_keys;
        if ($opts{shard_num} && $opts{shard_num} eq 'all') {
            delete $opts{shard_num};
            $shard_keys = { map { $_ => $keys } (1 .. $box->iproto->get_shard_count()) };
        }
    }
    $opts{type} = 'select';
    $opts{use_index} = $index->name;
    $opts{inplace} = 1;
    my @request = $shard_keys ? map +{ %opts, keys => $shard_keys->{$_}, shard_num => $_ }, keys %$shard_keys : { %opts, keys => $keys };
    my $response = $box->bulk(\@request);
    my @alltuples;
    foreach my $resp (@$response) {
        if ($resp->{error} != MR::Tarantool::Box::XS::ERR_CODE_OK) {
            if ($noraise) {
                my $count = @{$resp->{keys}};
                cluck "Failed to select $class, $count items are unavailable: $resp->{error}";
            } else {
                confess "Failed to select $class: $resp->{error}";
            }
        }
        my $tuples = $resp->{tuples};
        foreach my $tuple (@$tuples) {
            $tuple->{shard_num} = $resp->{shard_num} if exists $resp->{shard_num};
            $tuple->{replica} = 1 if $resp->{replica};
            $tuple->{exists} = 1;
        }
        push @alltuples, @$tuples;
    }
    if (my $limit = $opts{limit}) {
        $#alltuples = $limit - 1 if @alltuples > $limit;
    }
    if (my $deserialize = $meta->deserialize) {
        $deserialize->(\@alltuples);
    }
    my @list = map $class->new($_), @alltuples;
    if ($wantarrayref) {
        return \@list;
    } else {
        cluck sprintf "Select returned %d rows when only one row was expected", scalar @list if @list > 1;
        return $list[0];
    }
}

sub insert {
    my ($self) = @_;
    confess "Can't insert readonly data" if $self->readonly;
    @{$self->_update_ops} = ();
    my $meta = $self->meta;
    my %data = map { my $name = $_->name; $name => $self->$name } $meta->get_all_fields();
    if (my $serialize = $meta->serialize) {
        $serialize->([\%data]);
    }
    my $shard_num = $self->shard_num;
    my $response = $meta->box->do({
        type  => 'insert',
        tuple => \%data,
        $shard_num ? (shard_num => $shard_num) : (),
    });
    if ($response->{error} != MR::Tarantool::Box::XS::ERR_CODE_OK) {
        my $class = ref $self;
        die "Failed to insert $class: $response->{error}";
    }
    return;
}

sub update {
    my ($self) = @_;
    my $ops = $self->_update_ops;
    return unless @$ops;
    confess "Can't update readonly data" if $self->readonly;
    my $meta = $self->meta;
    if ($meta->serialize) {
        foreach my $op (@$ops) {
            my $attr = $meta->get_attribute($op->[0]);
            if (my $serialize = $attr->serialize) {
                local $_ = $op->[2];
                $op->[2] = $serialize->($_);
            }
        }
    }
    my $primary_key = $meta->primary_key;
    my $shard_num = $self->shard_num;
    my $response = $meta->box->do({
        type => 'update',
        key  => $self->$primary_key,
        ops  => $ops,
        $shard_num ? (shard_num => $shard_num) : (),
    });
    @$ops = ();
    if ($response->{error} != MR::Tarantool::Box::XS::ERR_CODE_OK) {
        my $class = ref $self;
        die "Failed to update $class: $response->{error}";
    }
    return;
}

sub delete {
    my ($self) = @_;
    confess "Can't delete readonly data" if $self->readonly;
    @{$self->_update_ops} = ();
    my $meta = $self->meta;
    my $primary_key = $meta->primary_key;
    my $shard_num = $self->shard_num;
    my $response = $meta->box->do({
        type => 'delete',
        key  => $self->$primary_key,
        $shard_num ? (shard_num => $shard_num) : (),
    });
    if ($response->{error} != MR::Tarantool::Box::XS::ERR_CODE_OK) {
        my $class = ref $self;
        die "Failed to delete $class: $response->{error}";
    }
    return;
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
