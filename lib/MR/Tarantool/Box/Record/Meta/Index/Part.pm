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

has index_meta => (
    is  => 'ro',
    isa => 'MR::Tarantool::Box::Record::Meta::Index::Base',
    lazy    => 1,
    default => sub {
        my ($self) = @_;
        my $index_name = $self->index;
        my $associated_class = $self->associated_class;
        my $index = first { $_->name eq $index_name } @{$associated_class->indexes};
        confess "Index $index not exists" unless $index;
        return $index;
    }
);

has '+fields' => (
    lazy    => 1,
    default => sub { [ @{$_[0]->index_meta->fields}[0 .. $_[0]->count - 1] ] },
);

has '+uniq' => (
    default => 0,
);

has '+shard_by' => (
    lazy    => 1,
    default => sub { $_[0]->index_meta->shard_by },
);

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
