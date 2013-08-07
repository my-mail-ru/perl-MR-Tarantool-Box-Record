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
        confess "'sequence_id' should be configured to use sequence" unless $field->primary_key;
        return $field->associated_class->namespace;
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
    $associated_class->add_method($self->next_method, sub {
        my $response = $box->do({
            type => 'update',
            key  => $id,
            ops  => [ [ value => num_add => 1 ] ],
            want_result => 1,
        });
        confess "Failed to get next sequence value for '$field_name': $response->{error}"
            unless $response->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK;
        confess "Sequence for '$field_name' not found in box" unless $response->{tuple};
        return $response->{tuple}->{value};
    });
    $associated_class->add_method($self->max_method, sub {
        my $response = $box->do({
            type => 'select',
            keys => [ $id ],
        });
        confess "Failed to get current sequence value for '$field_name': $response->{error}"
            unless $response->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK;
        confess "Sequence for '$field_name' not found in box" unless @{$response->{tuples}};
        return $response->{tuples}->[0]->{value};
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
    my ($self) = @_;
    my $response = $self->box->do({
        type   => 'insert',
        action => 'add',
        tuple  => [ $self->id, 0 ],
    });
    my $field_name = $self->field;
    confess "Failed to initialize sequence for '$field_name': $response->{error}"
        unless $response->{error} == MR::Tarantool::Box::XS::ERR_CODE_OK;
    return;
}

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
