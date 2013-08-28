package MR::Tarantool::Box::Record::Trait::Index::Devel;

use Mouse::Util::TypeConstraints;
enum 'MR::Tarantool::Box::Record::Trait::Index::Devel::Type' => qw( HASH TREE );
no Mouse::Util::TypeConstraints;

use Mouse::Role;

has type => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::Record::Trait::Index::Devel::Type',
    lazy    => 1,
    default => sub { $_[0]->uniq && !$_[0]->multifield ? 'HASH' : 'TREE' },
);

has storage_size => (
    is  => 'ro',
    isa => 'Int',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my ($self) = @_;
        my $fillfactor = 0.5;
        # 8 is sizeof(void *), 4 is sizeof(u32), 2 is sizeof(u16)
        if ($self->type eq 'HASH') {
            my $field = $self->associated_class->get_attribute($self->fields->[0]);
            return (8 + ($self->index_field_type($field) eq 'STR' ? 8 : $field->size)) / $fillfactor;
        } elsif ($self->type eq 'TREE') {
            # old version: return ((8 + 4 + 8) * @{$self->fields}) / $fillfactor;
            my $sizeofkeys = 0;
            foreach my $name (@{$self->fields}) {
                my $field = $self->associated_class->get_attribute($name);
                $sizeofkeys += $self->index_field_type($field) eq 'STR' ? 2 + 8 : $field->size;
            }
            # internal tree pointers are always 32bit wide
            return (4 + 4 + 8 + $sizeofkeys) / $fillfactor;
        } else {
            confess sprintf "Unknown type %s", $self->type;
        }
    },
);

{
    my %INDEX_FIELD_TYPE = (
        map({ $_ => 'NUM64' } qw/Q q/),
        map({ $_ => 'NUM' } qw/L l/),
        map({ $_ => 'NUM16' } qw/S s/),
        map({ $_ => 'STR' } qw/& $/),
    );

    sub index_field_type {
        my ($self, $field) = @_;
        $field = $self->associated_class->get_attribute($field) if ref $self && !ref $field;
        my $format = $field->format;
        my $type = $INDEX_FIELD_TYPE{$format};
        confess "Indexes on fields with format '$format' are unsupported" unless defined $type;
        return $type;
    }
}

no Mouse::Role;

1;
