#!/usr/bin/perl
use Test::More tests => 27;
use strict;
use warnings;

my $conn = { dsn => "dbi:SQLite:dbname=testfile.sql" };

unlink "testfile.sql";
END { unlink "testfile.sql" }

use DBIx::Abstract;

my $dsn;

my $dbh;
eval {
    $dbh = DBIx::Abstract->connect($conn);
}; is(ref($dbh), 'DBIx::Abstract', 'connect dbname');


eval {
    $dbh->disconnect if $dbh;
    my $dbi = DBI->connect($conn->{'dsn'},$conn->{'user'},$conn->{'password'});
    $dbh = DBIx::Abstract->connect($dbi);
}; is(ref($dbh), 'DBIx::Abstract', 'connect with dbi object');

unlink('test.log');

eval {
    $dbh->disconnect if $dbh;
    $dbh = DBIx::Abstract->connect($conn,{
        loglevel=>5,
        logfile=>'test.log',
        });
}; is($@ || ref($dbh), 'DBIx::Abstract', 'connect db');

eval {
    $dbh->disconnect if $dbh;
    $dbh->reconnect if $dbh;
};

ok( ! $@, "Reconnect: No error");
ok( $dbh, "Reconnect: Database handle exists");
ok( $dbh->connected(), "Reconnect: We are indeed connected." );

eval {
    my $dbih = $dbh->{'dbh'};
    my $dbh2 = DBIx::Abstract->connect($dbih);
    $dbh2->DESTROY();
};
ok( ! $@, "connect w/dbhandle and destroy: No error");
ok( $dbh, "connect w/dbhandle and destroy: Database handle exists");
ok( $dbh->connected(), "connect w/dbhandle and destroy: We are indeed connected." );

is( @{$dbh->{'CLONES'}}, 0, "no clones yet");
eval {
   eval {
       my $dbh2 = $dbh->clone();
       is( @{$dbh->{'CLONES'}}, 1, "one clone now");
   };
   is( @{$dbh->{'CLONES'}}, 0, "clone cleaned up");
};
ok( ! $@, "clone without error");

eval {
    $dbh->query('create table foo (id int null,name char(30) not null,value char(30) null)');
    $dbh->query('create table bar (id int null,foo_id int null,name char(30) not null)');
}; is($@,'','create');

my $test_rows = 4;
eval {
    for ( 1..$test_rows ) {
        $dbh->insert('foo',{id=>$_,name=>"test$_",value=>"value$_"});
        $dbh->insert('bar',{id=>$_,foo_id=>($test_rows+1)-$_,name=>"test$_"});
    }
}; is($@,'','insert');

eval {
    $dbh->update('foo',{name=>'blat', value=>'bonk'},{id=>2});
}; is($@,'','update');

my $count = 0;
eval {
    $dbh->select('*','foo',{id=>['<',10]});
    $dbh->rows;
    while (my @foo = $dbh->fetchrow_array) { $count ++ }
}; 
ok( !$@, "select without exception" );
is( $count, $test_rows, "select ($count==$test_rows)" );


eval {
    my @foo; @foo = ({id=>['<',10]},'and',\@foo);
    $dbh->select('*','foo',\@foo);
    if ($dbh->rows) {
        while (my @foo = $dbh->fetchrow_array) {  }
    }
}; is($@?1:0,1,'circular where');

eval {
    $dbh->select('*','foo',[{id=>['<',10]},'and',[{name=>'blat'},'or',{value=>'bonk'}]]);
    $dbh->rows;
    while (my @foo = $dbh->fetchrow_array) { }
}; is($@,'','select with complex where');

eval {
    $dbh->select({
        fields=>'count(foo.id)',
        tables=>'foo,bar',       
        'join'=>[
                 'foo.id = bar.foo_id', 
                 ],
        where=>{'foo.id'=>['<',10]},
        group=>'bar.name',
        });
    if ($dbh->rows) {
        while (my @foo = $dbh->fetchrow_array) { }
    }
}; is($@,'','select with join');

eval {
    $dbh->delete('foo',{id=>['like','%']});
}; is((!$@ and $test_rows==$dbh->rows)?1:0,1,'delete');

eval {
    $dbh->query('drop table foo');
    $dbh->query('drop table bar');
}; ok( !$@,'drop');

ok( $dbh->connected, "verified connection" );

eval { 
    $dbh->disconnect;
}; ok( ! $@,'disconnect');

ok( ! $dbh->connected, "verified disconnection" );

if (open(LOG,'test.log')) {
    my @log = <LOG>;
    close(LOG);
    my @data;
    my $ignore = 0;
    while (<DATA>) {
        if (/^[^\t]+\t0\t([^\t]+)\tSTART\n$/ and $1 ne $$conn{'dialect'}) {
            $ignore = $1;
        } elsif ($ignore and /^[^\t]+\t0\t$ignore\tEND\n$/) {
            $ignore = 0;
        } elsif (!$ignore) {
            push(@data,$_);
        }
    }
    foreach (\@log,\@data) {
        map({s/^[^\t]+/DATE/g} @$_);
        map({s/^(DATE\t5\t(?:Rec|C)onnect\t).*$/$1CONNECT ARGS/} @$_);
        map({s/^(DATE\t5\tconnected\t)\n$/${1}0\n/} @$_);
    }
    if (is_deeply( \@log, \@data, "SQL log matches expectations" )) {
        unlink('test.log');
    }
}

__DATA__
Tue Aug 23 12:40:28 2011	5	Option change	logfile		test.log
Tue Aug 23 12:40:28 2011	5	Connect	dsn=>dbi:SQLite:dbname=testfile.sql
Tue Aug 23 12:40:28 2011	5	connected	
Tue Aug 23 12:40:28 2011	5	reconnect	success
Tue Aug 23 12:40:28 2011	5	Reconnect
Tue Aug 23 12:40:28 2011	5	connected	1
Tue Aug 23 12:40:28 2011	5	connected	1
Tue Aug 23 12:40:28 2011	5	Cloned
Tue Aug 23 12:40:28 2011	3	create table foo (id int null,name char(30) not null,value char(30) null)
Tue Aug 23 12:40:28 2011	3	create table bar (id int null,foo_id int null,name char(30) not null)
Tue Aug 23 12:40:29 2011	1	INSERT INTO foo ( value, name, id) VALUES ('value1', 'test1', '1')
Tue Aug 23 12:40:29 2011	1	INSERT INTO bar ( name, foo_id, id) VALUES ('test1', '4', '1')
Tue Aug 23 12:40:29 2011	1	INSERT INTO foo ( value, name, id) VALUES ('value2', 'test2', '2')
Tue Aug 23 12:40:29 2011	1	INSERT INTO bar ( name, foo_id, id) VALUES ('test2', '3', '2')
Tue Aug 23 12:40:29 2011	1	INSERT INTO foo ( value, name, id) VALUES ('value3', 'test3', '3')
Tue Aug 23 12:40:30 2011	1	INSERT INTO bar ( name, foo_id, id) VALUES ('test3', '2', '3')
Tue Aug 23 12:40:30 2011	1	INSERT INTO foo ( value, name, id) VALUES ('value4', 'test4', '4')
Tue Aug 23 12:40:30 2011	1	INSERT INTO bar ( name, foo_id, id) VALUES ('test4', '1', '4')
Tue Aug 23 12:40:30 2011	1	UPDATE foo SET value='bonk', name='blat' WHERE id = '2'
Tue Aug 23 12:40:30 2011	2	SELECT * FROM foo WHERE id < '10'
Tue Aug 23 12:40:30 2011	5	rows
Tue Aug 23 12:40:30 2011	4	fetchrow_array
Tue Aug 23 12:40:30 2011	4	fetchrow_array
Tue Aug 23 12:40:30 2011	4	fetchrow_array
Tue Aug 23 12:40:30 2011	4	fetchrow_array
Tue Aug 23 12:40:30 2011	4	fetchrow_array
Tue Aug 23 12:40:30 2011	0	Where parser iterated too deep (limit of 20)
Tue Aug 23 12:40:30 2011	2	SELECT * FROM foo WHERE (id < '10') and ((name = 'blat') or (value = 'bonk'))
Tue Aug 23 12:40:30 2011	5	rows
Tue Aug 23 12:40:30 2011	4	fetchrow_array
Tue Aug 23 12:40:30 2011	4	fetchrow_array
Tue Aug 23 12:40:30 2011	2	SELECT count(foo.id) FROM foo,bar WHERE (foo.id < '10') and ( foo.id = bar.foo_id ) GROUP BY bar.name
Tue Aug 23 12:40:30 2011	5	rows
Tue Aug 23 12:40:30 2011	1	DELETE FROM foo WHERE id like '%'
Tue Aug 23 12:40:30 2011	5	rows
Tue Aug 23 12:40:30 2011	3	drop table foo
Tue Aug 23 12:40:30 2011	3	drop table bar
Tue Aug 23 12:40:31 2011	5	connected	1
Tue Aug 23 12:40:31 2011	5	connected	
