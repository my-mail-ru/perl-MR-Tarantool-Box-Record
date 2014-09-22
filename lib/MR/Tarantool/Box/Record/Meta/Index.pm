package MR::Tarantool::Box::Record::Meta::Index;

use Mouse;
extends 'MR::Tarantool::Box::Record::Meta::Index::Base';
with 'MR::Tarantool::Box::Record::Trait::Index::Devel';

has number => (
    is  => 'rw',
    isa => 'Maybe[Int]',
    lazy   => 1,
    default => sub { $_[0]->primary_key ? 0 : undef },
);

has '+fields' => (
    required => 1,
);

has primary_key => (
    is  => 'ro',
    isa => 'Bool',
);

has default => (
    is  => 'ro',
    isa => 'Bool',
    lazy    => 1,
    default => sub { $_[0]->primary_key },
);

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
