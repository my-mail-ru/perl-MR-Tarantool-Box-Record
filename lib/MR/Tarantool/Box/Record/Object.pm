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
    my $wantarrayref = ref $keys;
    my $noraise = delete $opts{noraise_unavailable};
    my $autocreate = delete $opts{autocreate};
    my $meta = $class->meta;
    my $attr = $meta->get_attribute($field);
    my $index = $attr->index
        or confess "Can't use field '$field' as an indexed field";
    my $type = $attr->type_constraint;
    my $serialize = $attr->serialize;
    my $keys_hash = ref $keys eq 'HASH' ? $keys : { 0 => ref $keys eq 'ARRAY' ? $keys : [ $keys ] };
    my @request = map {
        my $keys = $keys_hash->{$_};
        if ($type) {
            foreach my $key (@$keys) {
                cluck((defined $key ? "'$key'" : 'undef') . " doesn't look like $field") unless $type->check($key);
            }
        }
        if ($serialize) {
            $keys = [ map $serialize->($_), @$keys ];
        }
        +{
            %opts,
            type => 'select',
            keys => $keys,
            use_index => $index,
            inplace   => 1,
            $_ ? (shard_num => $_) : (),
        }
    } keys %$keys_hash;
    my $response = $meta->box->bulk(\@request);
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
    if (my $deserialize = $meta->deserialize) {
        $deserialize->(\@alltuples);
    }
    my @list = map $class->new($_), @alltuples;
    return $wantarrayref ? \@list : $list[0];
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
