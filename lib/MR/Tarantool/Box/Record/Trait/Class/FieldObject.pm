package MR::Tarantool::Box::Record::Trait::Class::FieldObject;

use Mouse::Role;

has object_tuples => (
    is  => 'ro',
    isa => 'Maybe[CodeRef]',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my %field = map { $_->name => $_->field }
            grep $_->does('MR::Tarantool::Box::Record::Trait::Attribute::FieldObject'), $self->get_all_attributes();
        return unless %field;
        return sub {
            my ($tuples, $map) = @_;
            foreach my $tuple (@$tuples) {
                foreach my $name (keys %$map) {
                    my $field = $field{$name} or next;
                    next unless exists $tuple->{$field};
                    $tuple->{$name} = $map->{$name}->{$tuple->{$field}};
                }
            }
            return;
        }
    }
);

sub add_field_object {
    my $self = shift;
    my $name = shift;
    my %args = @_;
    push @{$args{traits}}, 'MR::Tarantool::Box::Record::Trait::Attribute::FieldObject';
    $self->add_attribute($name, is => 'ro', %args);
    return;
}

no Mouse::Role;

1;
