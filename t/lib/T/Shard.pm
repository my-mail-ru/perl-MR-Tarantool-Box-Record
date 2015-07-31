package T::Shard;

use MR::Tarantool::Box::Record;

my @shard = split /,/, $ENV{SHARD_SERVERS};

iproto shards => {
    map { $_ + 1 => { masters => [ $shard[$_] ] } } (0 .. $#shard)
};

namespace 22;

has_field ID => (
    format => 'l',
    index  => 'primary',
    primary_key => 1,
    selector    => 'SelectByID',
);

has_field Bi1 => 'L';
has_field Bi2 => 'L';
has_index bi => (
    uniq     => 0,
    fields   => ['Bi1', 'Bi2'],
    selector => 'SelectByBi',
);
has_index_part bi_part => (
    index    => 'bi',
    selector => 'SelectByBiPart',
);

has_field F3 => 'l';
has_field F4 => 'l';
has_field F5 => '&';
has_field F6 => '&';
has_field F7 => '&';
has_field F8 => '&';
has_field F9 => '&';
has_field F10 => 'L';
has_field F11 => 'L';
has_field F12 => 'l';

no MR::Tarantool::Box::Record;
__PACKAGE__->meta->make_immutable();

1;
