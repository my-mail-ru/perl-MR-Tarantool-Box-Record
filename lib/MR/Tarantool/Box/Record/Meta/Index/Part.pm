package MR::Tarantool::Box::Record::Meta::Index::Part;

use Mouse;
use List::Util qw/first/;

extends 'MR::Tarantool::Box::Record::Meta::Index::Base';

has '+index' => (
    required => 1,
);

has count => (
    is  => 'ro',
    isa => 'Int',
    default => 1,
);

has '+fields' => (
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $index_name = $self->index;
        my $associated_class = $self->associated_class;
        my $index = first { $_->name eq $index_name } @{$associated_class->indexes};
        confess "Index $index not exists" unless $index;
        return [ @{$index->fields}[0 .. $self->count - 1] ];
    },
);

has '+uniq' => (
    default => 0,
);

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
