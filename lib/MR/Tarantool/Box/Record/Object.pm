package MR::Tarantool::Box::Record::Object;

use Mouse;
use Carp 'cluck';
use MR::Tarantool::Box::XS;
use overload ();

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
    my $object_map;
    if (delete $opts{by_object} && (my $deobject_keys = $index->deobject_keys)) {
        ($keys, $object_map) = $deobject_keys->($keys);
    }
    my $shard_keys;
    my $prepare_keys = $index->prepare_keys;
    if (ref $keys eq 'HASH') {
        confess "option 'shard_num' shouldn't be used with \$keys passed as a HASHREF" if exists $opts{shard_num};
        $shard_keys = { map { $_ => $prepare_keys ? $prepare_keys->($keys->{$_}) : $keys->{$_} } keys %$keys };
    } else {
        if (!exists $opts{shard_num} && (my $shard_by = $index->shard_by)) {
            $opts{shard_num} = $class->$shard_by(@$keys);
        }
        $keys = $prepare_keys->($keys) if $prepare_keys;
        if ($opts{shard_num} && $opts{shard_num} eq 'all') {
            delete $opts{shard_num};
            $shard_keys = { map { $_ => $keys } (1 .. $box->iproto->get_shard_count()) };
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
    if (my $object_tuples = $index->object_tuples) {
        $object_tuples->(\@alltuples, $object_map) if $object_map;
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

sub _create_default {
    my ($class, $key, $value) = @_;
    return { $key => $value };
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
