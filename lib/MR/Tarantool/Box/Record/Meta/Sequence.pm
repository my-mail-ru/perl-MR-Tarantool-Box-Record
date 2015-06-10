package MR::Tarantool::Box::Record::Meta::Sequence;

use Mouse;

has associated_field => (
    is  => 'rw',
    isa => 'Mouse::Meta::Attribute',
    weak_ref => 1,
);

has field => (
    is  => 'ro',
    isa => 'Str',
    required => 1,
);

has id => (
    is  => 'ro',
    isa => 'Int',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $field = $self->associated_field;
        my $associated_class = $field->associated_class;
        confess "'sequence_id' should be configured to use sequence" unless grep { $field->name eq $_ } @{$associated_class->primary_key->fields};
        return $associated_class->namespace;
    },
);

has iproto => (
    is  => 'ro',
    isa => 'MR::IProto::XS',
    lazy    => 1,
    default => sub { $_[0]->associated_field->associated_class->sequence_iproto },
);

has namespace => (
    is  => 'ro',
    isa => 'Int',
    lazy    => 1,
    default => sub { $_[0]->associated_field->associated_class->sequence_namespace },
);

has box => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::XS',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $field = $self->associated_field;
        my $format = $field->format;
        confess "Sequenced field format should be one of: Q, q, L, l" unless $format =~ /^[QqLl]$/;
        my $box_class = $field->associated_class->box_class;
        return $box_class->new(
            iproto    => $self->iproto,
            namespace => $self->namespace,
            fields    => [qw/ id value /],
            format    => 'L' . $format,
            indexes   => [{ name => 'primary', keys => ['id'], default => 1 }],
        );
    },
);

has next_method => (
    is  => 'ro',
    isa => 'Str',
    lazy    => 1,
    default => sub { '_get_next_' . $_[0]->field },
);

has max_method => (
    is  => 'ro',
    isa => 'Str',
    lazy    => 1,
    default => sub { 'get_max_' . $_[0]->field },
);

sub install_methods {
    my ($self) = @_;
    my $box = $self->box;
    my $field_name = $self->field;
    my $id = $self->id,
    my $associated_class = $self->associated_field->associated_class;
    my $microsharding = $self->associated_field->microsharding;
    my $bits = $associated_class->microshard_bits;
    $associated_class->add_method($self->next_method, sub {
        my $shard_num = $microsharding ? $_[0]->shard_num : undef;
        my $response = $box->do({
            type => 'update',
            key  => $id,
            ops  => [ [ value => num_add => $microsharding ? 65536 : 1 ] ], # FIXME
            want_result => 1,
            $microsharding ? (shard_num => $shard_num) : (),
        });
        confess "Failed to get next sequence value for '$field_name': $response->{error}"
            unless $response->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK;
        confess "Sequence for '$field_name' not found in box" unless $response->{tuple};
        my $value = $response->{tuple}->{value};
        return $microsharding ? (($value << $bits) | $shard_num) : $value;
    });
    $associated_class->add_method($self->max_method, sub {
        my $shard_num = $microsharding ? $_[1] : undef;
        my $response = $box->do({
            type => 'select',
            keys => [ $id ],
            $microsharding ? (shard_num => $shard_num) : (),
        });
        confess "Failed to get current sequence value for '$field_name': $response->{error}"
            unless $response->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK;
        confess "Sequence for '$field_name' not found in box" unless @{$response->{tuples}};
        confess "Select return more than one tuple for '$field_name'" if scalar @{$response->{tuples}} > 1;
        my $value = $response->{tuples}->[0]->{value};
        return $microsharding ? (($value << $bits) | $shard_num) : $value;
    });
    $self->associate_method($self->next_method);
    $self->associate_method($self->max_method);
    return;
}

sub associate_method {
    my ($self, $method_name) = @_;
    return;
}

sub initialize_sequence {
    my ($self, $start_value) = @_;
    $start_value = 0 unless ( $start_value && $start_value =~ m/^\d+$/ );
    my $bits = $self->associated_field->associated_class->microshard_bits;
    my $max_shard = 1 << $bits;
    my @request = map +{
        type      => 'insert',
        action    => 'add',
        tuple     => [ $self->id, $start_value ],
        shard_num => $_,
    }, (1 .. $max_shard);
    my $response = $self->box->bulk(\@request);
    my $field_name = $self->field;
    my @error = map { "$request[$_]->{shard_num}: $response->[$_]->{error}" }
        grep { $response->[$_] != MR::Tarantool::Box::XS::ERR_CODE_OK } (0 .. $#request);
    confess "Failed to initialize sequence for '$field_name': " . join ', ', @error if @error;
    return;
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
