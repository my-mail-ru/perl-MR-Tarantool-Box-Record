use strict;
use warnings;
use Test::More tests => 4;
use T::Dispatch;
use T::Shard;

my $user = T::Dispatch->SelectByID(158809549);
is($user->Email, 'my_testing100@mail.ru', "SelectByID");
$user = T::Dispatch->SelectByEmail('my_testing100@mail.ru');
is($user->ID, 158809549, "SelectByEmail");

my $users = T::Shard->SelectByBirthday([1, 2], shard_num => 'all', limit => 10);
is(scalar @$users, 10 * T::Shard->meta->box->iproto->get_shard_count(), "SelectByBi");

$users = T::Shard->SelectByBirthdayMonth(2, shard_num => 'all', limit => 10);
is(scalar @$users, 10 * T::Shard->meta->box->iproto->get_shard_count(), "SelectByBiPart");
