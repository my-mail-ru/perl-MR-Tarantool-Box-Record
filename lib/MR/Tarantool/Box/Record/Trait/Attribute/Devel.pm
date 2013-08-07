package MR::Tarantool::Box::Record::Trait::Attribute::Devel;

use Mouse::Role;

has min_storage_size => (
    is  => 'ro',
    isa => 'Int',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my $size = $_[0]->min_size;
        $size += int($size / 0x80) + 1 if defined $size;
        return $size;
    },
);

has max_storage_size => (
    is  => 'ro',
    isa => 'Maybe[Int]',
    init_arg => undef,
    lazy     => 1,
    default  => sub {
        my $size = $_[0]->max_size;
        $size += int($size / 0x80) + 1 if defined $size;
        return $size;
    },
);


no Mouse::Role;

1;
