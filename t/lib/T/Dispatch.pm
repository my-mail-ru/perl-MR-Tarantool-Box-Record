package T::Dispatch;

use MR::Tarantool::Box::Record;
use MR::IProto::XS;

iproto masters => [$ENV{DISPATCH_SERVER}];

namespace 0;

has_field ID => (
    format   => 'l',
    selector => 'SelectByID',
    index    => 'primary_id',
    primary_key => 1,
);

has_field Email => (
    format   => '&',
    selector => 'SelectByEmail',
    index    => 'primary_email',
);

has_field D2 => (
    format => 'S',
    deserialize => sub { $_ ? $_ : ($_[1]->{ID} % 6) + 1 },
);
has_field D3 => 'S';
has_field D4 => '&';
has_field D5 => 'L';

no MR::Tarantool::Box::Record;
__PACKAGE__->meta->make_immutable();

1;
