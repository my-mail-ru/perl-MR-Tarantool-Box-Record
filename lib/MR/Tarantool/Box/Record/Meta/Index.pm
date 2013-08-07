package MR::Tarantool::Box::Record::Meta::Index;

use Mouse;
extends 'MR::Tarantool::Box::Record::Meta::Index::Base';
with 'MR::Tarantool::Box::Record::Trait::Index::Devel';

has number => (
    is  => 'rw',
    isa => 'Int',
);

has '+fields' => (
    required => 1,
);

has default => (
    is  => 'ro',
    isa => 'Bool',
);

no Mouse;
__PACKAGE__->meta->make_immutable();

1;
