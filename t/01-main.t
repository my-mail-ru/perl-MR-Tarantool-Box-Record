use strict;
use warnings;
use Test::More tests => 2;
use T::Dispatch;

my $user = T::Dispatch->SelectByID(158809549);
is($user->Email, 'my_testing100@mail.ru', "SelectByID");
$user = T::Dispatch->SelectByEmail('my_testing100@mail.ru');
is($user->ID, 158809549, "SelectByEmail");
