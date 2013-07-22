package MR::Tarantool::Box::Record::Trait::Class;

use Mouse::Role;
use MR::Tarantool::Box::XS;
use List::MoreUtils qw/uniq/;

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
    default => sub {
        my ($self) = @_;
        my (@fields, $format);
        foreach my $attr ($self->get_all_fields()) {
            push @fields, $attr->name;
            $format .= $attr->format;
        }
        my @indexes = map +{
            name    => $_->name,
            keys    => $_->fields,
            default => $_->default,
        }, @{$self->indexes};
        return MR::Tarantool::Box::XS->new(
            iproto    => $self->iproto,
            namespace => $self->namespace,
            fields    => \@fields,
            format    => $format,
            indexes   => \@indexes,
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

has indexes => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Meta::Index]',
    default => sub { [] },
);

has index_by_name => (
    is  => 'ro',
    isa => 'HashRef[MR::Tarantool::Box::Record::Meta::Index]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my %data;
        foreach my $index (@{$self->indexes}) {
            $data{$index->name} ||= $index;
            $data{join ',', @{$index->fields}} ||= $index;
        }
        return \%data;
    },
);

has fields_accessors => (
    is  => 'ro',
    isa => 'ArrayRef',
    lazy    => 1,
    default => sub { [ map uniq(grep defined, $_->get_read_method(), $_->get_write_method(), values %{$_->mutators}), $_[0]->get_all_fields() ] },
);

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

sub add_index {
    my $self = shift;
    my $index;
    if (blessed $_[0]) {
        $index = shift;
        $index->associated_class($self);
    } else {
        my $name = shift;
        my %args = @_ == 0 ? (fields => $name) : @_ == 1 ? (fields => shift) : @_;
        $args{name} = $name unless ref $name;
        $args{fields} = [ $args{fields} ] unless ref $args{fields};
        $args{associated_class} = $self;
        $index = MR::Tarantool::Box::Record::Meta::Index->new(\%args);
    }
    $index->install_selectors();
    push @{$self->indexes}, $index;
    return;
}

after make_immutable => sub {
    $_[0]->primary_key;
    $_[0]->box;
    $_[0]->serialize;
    $_[0]->deserialize;
    $_[0]->index_by_name;
    $_[0]->fields_accessors;
    return;
};

sub get_field_list {
    return grep $_[0]->get_attribute($_)->does('MR::Tarantool::Box::Record::Trait::Attribute::Field'), $_[0]->get_attribute_list();
}

sub get_all_fields {
    return grep $_->does('MR::Tarantool::Box::Record::Trait::Attribute::Field'), $_[0]->get_all_attributes();
}

no Mouse::Role;

1;
