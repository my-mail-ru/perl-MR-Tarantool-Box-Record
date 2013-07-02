package MR::Tarantool::Box::Record::Trait::Class;

use Mouse::Role;
use MR::Tarantool::Box::XS;

use Mouse::Util::TypeConstraints;
type 'MR::Tarantool::Box::Record::Trait::Class::Box' => where {
    my $class;
    if (ref $_) {
        $class = blessed $_ or return;
    } else {
        return unless Mouse::Util::is_class_loaded($_);
        $class = $_;
    }
    return $class->isa('MR::Tarantool::Box::XS');
};
no Mouse::Util::TypeConstraints;

has iproto => (
    is  => 'rw',
    isa => 'MR::IProto::XS',
);

has namespace => (
    is  => 'rw',
    isa => 'Int',
);

has fields => (
    is  => 'ro',
    isa => 'ArrayRef[Str]',
    lazy    => 1,
    default => sub { [ map $_->name, $_[0]->get_all_fields() ] },
);

has format => (
    is  => 'rw',
    isa => 'Str',
    lazy    => 1,
    default => sub { join '', map $_->format, $_[0]->get_all_fields() },
);

has indexes => (
    is  => 'ro',
    isa => 'ArrayRef[HashRef]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        return [
            map +{
                name => $_->index,
                keys => [ $_->name ],
                default => $_->primary_key,
            }, grep $_->index, $self->get_all_fields()
        ];
    },
);

has primary_key => (
    is  => 'ro',
    isa => 'Str',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        foreach my $attr ($self->get_all_fields()) {
            return $attr->name if $attr->primary_key;
        }
        die sprintf "No primary_key defined for %s", $self->name;
    },
);

has box => (
    is  => 'rw',
    isa => 'MR::Tarantool::Box::Record::Trait::Class::Box',
    lazy    => 1,
    builder => sub {
        my ($self) = @_;
        return MR::Tarantool::Box::XS->new(
            iproto    => $self->iproto,
            namespace => $self->namespace,
            fields    => $self->fields,
            format    => $self->format,
            indexes   => $self->indexes,
        );
    },
);

foreach my $attrname ('serialize', 'deserialize') {
    has $attrname => (
        is  => 'ro',
        isa => 'Maybe[CodeRef]',
        lazy    => 1,
        default => sub {
            my ($self) = @_;
            my %funcs;
            foreach my $attr ($self->get_all_fields()) {
                $funcs{$attr->name} = $attr->$attrname if $attr->$attrname;
            }
            return unless %funcs;
            return sub {
                my ($tuples) = @_;
                foreach my $tuple (@$tuples) {
                    while (my ($name, $func) = each %funcs) {
                        local $_ = $tuple->{$name};
                        $tuple->{$name} = $func->($_, $tuple);
                    }
                }
                return;
            }
        },
    );
}

sub add_field {
    my $self = shift;
    my $name = shift;
    my %args = @_ == 1 ? (format => shift) : @_;
    push @{$args{traits}}, 'MR::Tarantool::Box::Record::Trait::Attribute::Field';
    $self->add_attribute(
        $name => %args,
        is    => 'rw',
    );
    return;
}

after make_immutable => sub {
    $_[0]->fields;
    $_[0]->format;
    $_[0]->indexes;
    $_[0]->primary_key;
    $_[0]->box;
    $_[0]->serialize;
    $_[0]->deserialize;
};

sub get_field_list {
    return grep $_[0]->get_attribute($_)->does('MR::Tarantool::Box::Record::Trait::Attribute::Field'), $_[0]->get_attribute_list();
}

sub get_all_fields {
    return grep $_->does('MR::Tarantool::Box::Record::Trait::Attribute::Field'), $_[0]->get_all_attributes();
}

no Mouse::Role;

1;
