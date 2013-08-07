package MR::Tarantool::Box::Record::Trait::Class;

use Mouse::Role;
use MR::IProto::XS;
use MR::Tarantool::Box::XS;
use MR::Tarantool::Box::Record::Meta::Index;
use MR::Tarantool::Box::Record::Meta::Index::Part;
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

with 'MR::Tarantool::Box::Record::Trait::Class::Devel';

has iproto_class => (
    is  => 'rw',
    isa => 'ClassName',
    default => 'MR::IProto::XS',
);

has _iproto => (
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

has box_class => (
    is  => 'rw',
    isa => 'ClassName',
    default => 'MR::Tarantool::Box::XS',
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
        return $self->box_class->new(
            iproto    => $self->_iproto,
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
                        next unless exists $tuple->{$name};
                        local $_ = $tuple->{$name};
                        $tuple->{$name} = $func->($_, $tuple);
                    }
                }
                return;
            }
        },
    );
}

has fields => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Trait::Attribute::Field]',
    default => sub { [] },
);

has indexes => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Meta::Index]',
    default => sub { [] },
);

has index_parts => (
    is  => 'ro',
    isa => 'ArrayRef[MR::Tarantool::Box::Record::Meta::Index::Part]',
    default => sub { [] },
);

has index_by_name => (
    is  => 'ro',
    isa => 'HashRef[MR::Tarantool::Box::Record::Meta::Index::Base]',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my %data;
        foreach my $index (@{$self->indexes}, @{$self->index_parts}) {
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

has sequence_iproto => (
    is  => 'rw',
    isa => 'MR::IProto::XS',
    lazy    => 1,
    default => sub { confess "'sequence_iproto' should be configured to use sequences" },
);

has sequence_namespace => (
    is  => 'rw',
    isa => 'Int',
    lazy    => 1,
    default => sub { confess "'sequence_namespace' should be configured to use sequences" },
);

sub set_iproto {
    my $self = shift;
    my $iproto = @_ == 1 ? shift : $self->iproto_class->new(@_);
    $self->_iproto($iproto);
    return;
}

sub shard_by {
    my ($self, $code) = @_;
    $self->add_attribute('+shard_num',
        lazy    => 1,
        default => $code,
    );
    return;
}

sub add_field {
    my $self = shift;
    my $name = shift;
    my %args = @_ == 1 ? (format => shift) : @_;
    push @{$args{traits}}, 'MR::Tarantool::Box::Record::Trait::Attribute::Field';
    $self->add_attribute($name, is => 'rw', %args);
    return;
}

sub add_field_object {
    my $self = shift;
    my $name = shift;
    my %args = @_;
    push @{$args{traits}}, 'MR::Tarantool::Box::Record::Trait::Attribute::FieldObject';
    $self->add_attribute($name, is => 'ro', %args);
    return;
}

around add_attribute => sub {
    my ($orig, $self) = splice @_, 0, 2;
    my $attr = $self->$orig(@_);
    if ($attr->does('MR::Tarantool::Box::Record::Trait::Attribute::Field')) {
        my $number;
        my $fields = $self->fields;
        if (defined($number = $attr->number)) {
            confess "Field number $number already exists"
                if defined $fields->[$number] && $fields->[$number]->name ne $attr->name;
        } else {
            for ($number = 0; $number <= @$fields; $number++) {
                last unless defined $fields->[$number];
            }
            $attr->number($number);
        }
        $fields->[$number] = $attr;
    }
    return $attr;
};

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
    my $number;
    my $indexes = $self->indexes;
    if (defined($number = $index->number)) {
        confess "Index number $number already exists"
            if defined $indexes->[$number] && $indexes->[$number]->name ne $index->name;
    } else {
        for ($number = 0; $number <= @$indexes; $number++) {
            last unless defined $indexes->[$number];
        }
        $index->number($number);
    }
    $indexes->[$number] = $index;
    return;
}

sub add_index_part {
    my $self = shift;
    my $part;
    if (blessed $_[0]) {
        $part = shift;
        $part->associated_class($self);
    } else {
        my $name = shift;
        my %args = @_ == 1 ? (index => shift) : @_;
        $args{name} = $name;
        $args{associated_class} = $self;
        $part = MR::Tarantool::Box::Record::Meta::Index::Part->new(\%args);
    }
    $part->install_selectors();
    push @{$self->index_parts}, $part;
    return;
}

after make_immutable => sub {
    my ($self) = @_;
    $self->_validate_fields();
    $self->_validate_indexes();
    $self->primary_key;
    $self->box;
    $self->serialize;
    $self->deserialize;
    $self->index_by_name;
    $self->fields_accessors;
    return;
};

sub get_field_list {
    return map $_->name, @{$_[0]->fields};
}

sub get_all_fields {
    return @{$_[0]->fields};
}

sub initialize_sequence {
    my ($self, $field_name) = @_;
    my $field = $self->get_attribute($field_name)
        or confess "Field $field_name not exists";
    my $sequence = $field->sequence
        or confess "Field $field_name has no sequence";
    $sequence->initialize_sequence();
    return;
}

sub _validate_fields {
    my ($self) = @_;
    my $fields = $self->fields;
    my @empty = grep !defined($fields->[$_]), (0 .. $#$fields);
    return unless @empty;
    my $numbers = join ', ', @empty;
    confess "Fields numbers $numbers are not defined";
}

sub _validate_indexes {
    my ($self) = @_;
    my $indexes = $self->indexes;
    my @empty = grep !defined($indexes->[$_]), (0 .. $#$indexes);
    return unless @empty;
    my $numbers = join ', ', @empty;
    confess "Indexes number $numbers are not defined";
}

no Mouse::Role;

1;
