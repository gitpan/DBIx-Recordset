# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

use strict ;
use vars qw{ *set1 *set2 *set3 *set4 *set5 *set6 *set7 *set8 *set9 *set10
             *set11 *set12 *set13 *set14 *set15 *set16 *set17 *set18 *set19 *set20
             *set1_ *set20c *set13h %set15h
             @TestData @TestFields %TestCheck %hTestFields1 %hTestIds1 @TestSetup @TestIds
             @Table $Driver $DSN $User $Password
             @drivers %Drivers 
             $dbh $drv %errcnt $err $rc
             $errors $fatal $loaded
             $Join} ;


BEGIN { $| = 1;  $fatal = 1 ; print "\nLoading...                "; }

END {
    print "not ok 1\n" unless $loaded ;
    print "\nTest terminated with fatal error! Look at test.log\n" if ($fatal) ;
    }


use DBIx::Recordset ;

$loaded = 1;
print "ok\n";

######################### End of black magic.


my $configfile = 'test/Config.pl' ;

#################################################

sub printlog
    {
    print $_[0] ;
    print LOG $_[0] ;
    }


sub printlogf
    {
    formline ('@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<... ', $_[0]) ;
    printlog $^A ;	
    $^A = '' ;
    }


#################################################

sub Check 

    {
    my ($ids, $fields, $set, $idfield, $hash) = @_ ;
    my $id ;
    my $field ;
    my $i ;
    my $n ;
    my $is ;
    my $should ;
    my %setid ;
    my $dat ;
    my $v ;
    my $k ;


    print LOG "CHK-IDS: @$ids\n" ;
    
    $idfield ||= 'id' ;

    if (($dat = $$set[0]) && defined ($hash))
        {
        $n = $#$fields + 1 ;
        $i = 0 ;
        $v = $$dat{$idfield} ;
        print LOG "Check Hash $idfield = $v : $$hash{$idfield}\n" ;
        while (($k, $v) = each (%$dat))
            {
            $i++ ;
            print LOG "Field: $k Array: $v Hash: $$hash{$k}\n" ;
            if ($v ne $$hash{$k})
                {
                printlog "ERROR\n" ;
                printlog "Field: $k Array: $v Hash: $$hash{$k}\n" ;
                $errors++ ;
                return 1 ;
                }
            }

        if ($i != $n)
            {
            printlog "ERROR\n" ;
            printlog "Wrong number of fields in ::Row (get $i, expected $n)\n" ;
            $errors++ ;
            return 1 ;
            } 

        $i = 0 ;
        while (($k, $v) = each (%$hash))
            {
            $i++ ;
            if ($v ne $$dat{$k})
                {
                printlog "ERROR\n" ;
                printlog "Field: $k Array: $$dat{$k} Hash: $v\n" ;
                $errors++ ;
                return 1 ;
                }
            }

        if ($i != $n)
            {
            printlog "ERROR\n" ;
            printlog "Wrong number of fields in ::CurrRow (get $i, expected $n)\n" ;
            $errors++ ;
            return 1 ;
            } 

        }

    $i = 0 ;
    $n = $#$ids + 1 ;
    while ($dat = $$set[$i])
        {
        $v = $$dat{$idfield} ;
        $v =~ s/^(.*?)\s*$/$1/ ; 
        $setid{$v} = $i ;
        #print LOG "idfield =$idfield;$v;$i; \n" ;
        print LOG "CHK-DAT:" ;
        while (($k, $v) = each (%$dat))
            { $v ||= '' ; print LOG "$k=$v; " ; }
        #print "$idfield = $$dat{$idfield} = $i\n" ;
        $i++ ;
        print LOG "\n" ;
        }

    #print "get $i, expected $n\n" ;

    if ($i < $n)
        {
        printlog "ERROR\n" ;
        printlog "Get too few rows (get $i, expected $n)\n" ;
        $errors++ ;
        return 1 ;
        } 
    
    if ($i > $n)
        {
        printlog "ERROR\n" ;
        printlog "Get too many rows (get $i, expected $n)\n" ;
        $errors++ ;
        return 1 ;
        } 
    
    foreach $id (@$ids)
        {
        $dat = $$set[$setid{$id}] ;
        #print LOG "id =$id;$setid{$id};\n" ;
        foreach $field (@$fields)
            {
            if (exists ($TestCheck{$id}{$field}))
                {
                $should = $TestCheck{$id}{$field} ;
                }
            else
                {
                $should = $TestCheck{$TestCheck{$id}{'id'}}{$field} ;
                }
    
            $is     = $$dat{$field} || '' ;
            $is =~ /^(.*?)\s*$/ ;
            $is = $1 || '' ;

            print LOG "CHK-OK?: $idfield = $id; $field = <$is>; Should = <$should>\n" ;
             
            if ($should ne $is)
                {
                printlog "ERROR\n" ;
                printlog "$idfield     = $id\n" ;
                printlog "Field  = $field\n" ;
                printlog "Is     = $is\n" ;
                printlog "Should = $should\n" ;
                $errors++ ;
                return 1 ;
                }
            }
        }


    return 0 ;
    }


#################################################

sub AddTestRow

    {
    my ($tabno, $dat, $key) = @_ ;

    my $ex = 0 ;
    my $id ;
    my $v ;
    my $k ;

    $key ||= 'id' ;
    
    $id = undef ;
    $ex = exists ($$dat{$key}) ;
    if ($ex)
        { $id = $$dat{$key}  ; }
    else
        { $id = $$dat{"*$key"} ; }
    $id =~ s/\'(.*?)\'/$1/ ; 
    while (($k, $v) = each (%$dat))
        {
        $v =~ s/\'(.*?)\'/$1/ ; 
        $TestCheck{$id}{$k} = $v ;
        print LOG "TEST-DAT: Table $Table[$tabno] \$TestCheck{$id}{$k} = $v\n" ;
        if ($ex)
            {
            #$hTestFields{$k} = 1 ;
            $hTestFields1{$k} = 1 ;
            }
        }
    if ($ex)
        {
        #$hTestIds{$id} = 1 ;
        $hTestIds1{$id} = 1 ;
        }

    delete $$dat{"*$key"} ;
    
    $k = join (',', keys(%$dat)) ;
    $v = join (',', values(%$dat)) ;

    push (@TestSetup, "INSERT INTO $Table[$tabno] ($k) VALUES ($v)") if ($v && $k) ;

    }



sub AddTestRowAndId

    {
    my ($tabno, $dat, $key) = @_ ;
    
    my $id ;

    local %hTestIds1 ;
    local %hTestFields1 ;

    AddTestRow ($tabno, $dat, $key) ;

    foreach $id (@{$TestIds[$tabno]})
        {
        $hTestIds1{$id} = 1 ;
        }
            
    my @ids = keys %hTestIds1 ;
    $TestIds[$tabno] = \@ids ;
    }

sub DelTestRowAndId

    {
    my ($tabno, $id) = @_ ;
    
    my $tid ;

    delete $TestCheck{$id} ;

    local %hTestIds1 ;

    foreach $tid (@{$TestIds[$tabno]})
        {
        $hTestIds1{$tid} = 1 if ($tid ne $id) ;
        }
            
    my @ids = keys %hTestIds1 ;
    $TestIds[$tabno] = \@ids ;
    }

#################################################

sub AddTestData

    {
    my ($tabno, $key) = @_ ;

    my $dat ;

    local %hTestIds1 ;
    local %hTestFields1 ;
    my $ex = 0 ;

    $key ||= 'id' ;
    
    foreach $dat (@{$TestData[$tabno]})
        {
        AddTestRow ($tabno, $dat, $key) ;
        }
    
    
    my @ids = keys %hTestIds1 ;
    $TestIds[$tabno] = \@ids ;
    my @fld = keys %hTestFields1 ;
    $TestFields[$tabno] = \@fld ;
    }


#################################################

sub DoTest

    {

    $Driver      = $_[0] ;
    $DSN         = $_[1]  ; 
    $User        = $_[2] ;
    $Password    = $_[3] ;


    $Join =  (defined ($DBIx::Compat::Compat{$Driver}{SupportJoin}))?$DBIx::Compat::Compat{$Driver}{SupportJoin}:$DBIx::Compat::Compat{'*'}{SupportJoin} ;    

    @Table       = ('dbixrs1', 'dbixrs2', 'dbixrs3', 'dbixrs4') ;

    $errors = 0 ;
    
    open LOG, ">>test.log" or die "Cannot open test.log" ; 

    *DBIx::Recordset::LOG = \*LOG ; 
    $DBIx::Recordset::Debug = 2 ; 

    open (STDERR, ">&LOG") || die "Cannot redirect stderr" ;  
    #open (STDERR, ">dbi.log") || die "Cannot redirect stderr" ;  
    #DBI->trace(2) ;
    select (STDERR) ; $| = 1 ;
    select (LOG) ; $| = 1 ;
    select (STDOUT) ; $| = 1 ;

    printlog "\nUsing the following parameters for testing:\n" ;
    printlog "  DBD-Driver:  $Driver\n" ;
    printlog "  Database:    $DSN\n" ;
    printlog "  User:        " . ($User || '') . "\n" ;
    printlog "  Password:    " . ($Password || '') . "\n" ;
    

    my $t ;
    for $t (@Table)
        {
        printlog "  Table:       $t\n" ;
        }

    #printlog "host:        $Host\n" ;

    printlog "\n" ;


    $dbh = DBI->connect ("$DSN",$User, $Password) or die "Cannot connect to $DSN ($DBI::errstr)" ;

    printlog "  Driver does not support joins, skiping tests with multiple tables\n\n" if (!$Join) ;

no strict ;
    printlog "  DBI-Version: " . $DBI::VERSION . "\n" ;
    printlog "  DBD-Version: " . ${"DBD\:\:$Driver\:\:VERSION"} . "\n\n" ;
use strict ;

    printlogf "Creating the testtables";
    print LOG "\n--------------------\n" ;
    @TestSetup =
        (
        " DROP TABLE $Table[0]",
        " DROP TABLE $Table[1]",
        " DROP TABLE $Table[2]",
        " DROP TABLE $Table[3]",
    
        "CREATE TABLE $Table[0] ( id INT, name CHAR (20), value1 INT, addon CHAR (20) )",
        "CREATE TABLE $Table[1] ( id INTEGER, name2 CHAR(20), value2 INTEGER )",
        "CREATE TABLE $Table[2] ( value1 INTEGER, txt CHAR(20) )",
        "CREATE TABLE $Table[3] ( id INTEGER, typ CHAR(20) )",
        ) ;

    @TestData =
        (
            [
                { 'id' => 1 ,  'name' => "'First Name'",    'value1' => 9991,  'addon' => "'Is'" },
                { 'id' => 2 ,  'name' => "'Second Name'",   'value1' => 9992,  'addon' => "'it'" },
                { 'id' => 3 ,  'name' => "'Third Name'",    'value1' => 9993,  'addon' => "'it ok?'" },
                { 'id' => 4 ,  'name' => "'Fourth Name'",   'value1' => 9994,  'addon' => "'Or not??'" },
                { 'id' => 5 ,  'name' => "'Fivth Name'",    'value1' => 9995,  'addon' => "'Is'" },
                { 'id' => 6 ,  'name' => "'Sixth Name'",    'value1' => 9996,  'addon' => "'it'" },
                { 'id' => 7 ,  'name' => "'Seventh Name'",  'value1' => 9997,  'addon' => "'it ok?'" },
                { 'id' => 8 ,  'name' => "'Eighth Name'",   'value1' => 9998,  'addon' => "'Or not??'" },
                { 'id' => 9 ,  'name' => "'Ninth Name'",    'value1' => 9999,  'addon' => "'Is'" },
                { 'id' => 10,  'name' => "'Tenth Name'",    'value1' => 99910, 'addon' => "'it'" },
                { 'id' => 11,  'name' => "'Eleventh Name'", 'value1' => 99911, 'addon' => "'it ok?'" },
                { 'id' => 12,  'name' => "'Twelvth Name'",  'value1' => 99912, 'addon' => "'Or not??'" },
            ],
            [
                { 'id' => 1 ,  'name2' => "'First Name in Tab2'",  'value2' => 29991 },
                { 'id' => 2 ,  'name2' => "'Second Name in Tab2'", 'value2' => 29992 },
                { 'id' => 3 ,  'name2' => "'Third Name in Tab2'",  'value2' => 29993 },
                { 'id' => 4 ,  'name2' => "'Fourth Name in Tab2'", 'value2' => 29994 },
            ],
            [
                { '*id' => 1 ,  'txt' => "'First Item   (9991 )'", 'value1' => 9991, },
                { '*id' => 2 ,  'txt' => "'Second Item  (9992 )'", 'value1' => 9992, },
                { '*id' => 3 ,  'txt' => "'Third Item   (9993 )'", 'value1' => 9993, },
                { '*id' => 4 ,  'txt' => "'Fourth Item  (9994 )'", 'value1' => 9994, },
                { '*id' => 5 ,  'txt' => "'Fivth Item   (9995 )'", 'value1' => 9995, },
                { '*id' => 6 ,  'txt' => "'Sixth Item   (9996 )'", 'value1' => 9996, },
                { '*id' => 7 ,  'txt' => "'Seventh Item (9997 )'", 'value1' => 9997, },
                { '*id' => 8 ,  'txt' => "'Eighth Item  (9998 )'", 'value1' => 9998, },
                { '*id' => 9 ,  'txt' => "'Ninth Item   (9999 )'", 'value1' => 9999, },
                { '*id' => 10,  'txt' => "'Tenth Item   (99910)'", 'value1' => 99910,},
                { '*id' => 11,  'txt' => "'Eleventh Item(99911)'", 'value1' => 99911,},
                { '*id' => 12,  'txt' => "'Twelvth Item (99912)'", 'value1' => 99912,},
            ],

            [
                { 'id' => 1 , 'typ' => "'First item Type 1'" },
                { 'id' => 1 , 'typ' => "'First item Type 2'" },
                { 'id' => 1 , 'typ' => "'First item Type 3'" },
                { 'id' => 2 , 'typ' => "'Second item Type 1'" },
                { 'id' => 2 , 'typ' => "'Second item Type 2'" },
                { 'id' => 2 , 'typ' => "'Second item Type 3'" },
                { 'id' => 2 , 'typ' => "'Second item Type 4'" },
                { 'id' => 3 , 'typ' => "'Third item Type 1'" },
    #            { 'id' => 4 , 'typ' => "'Fours item Type 1'" },
            ],
        ) ;

    my $i ;

    for ($i = 0; $i <= $#Table - 1; $i++)
        {
        AddTestData ($i) ;
        }

    AddTestData (3, 'typ') ;

    #@AllTestIds = keys %hTestIds ;
    #@AllTestFields = keys %hTestFields ;

    my %count = ();
    my $element ;
    foreach $element (@{$TestFields[0]}) { $count{$element}++ }
    foreach $element (@{$TestFields[1]}) { $count{$element}++ }
    my @TestFields0_1 = keys %count ;


    #goto skip1 ;

    my $st ;
    my $rc ;

    foreach $st (@TestSetup)
        {
        $rc = $dbh -> do ($st) ;
        print LOG "$st ->($rc)\n" ;
        if (!$rc && $st =~ /^\S/)
            {
            die "Cannot do $st ($DBI::errstr)" ;
            } 
        }

    skip1:

    #$dbh->commit () ;
    $dbh->disconnect ; # or die "Cannot disconnect from $DSN ($DBI::errstr)" ;

    undef $dbh ;

    printlog "ok\n";

    #########################################################################################
    #
    # Start Tests
    #

    $errors = 0 ;

    # ---------------------

    printlogf "Setup Object for $Table[0]";
    print LOG "\n--------------------\n" ;

    $set1 = DBIx::Recordset->New ($DSN, $Table[0], $User, $Password) or die "not ok\n" ;
    tie @set1, 'DBIx::Recordset', $set1 ;
    tie %set1, 'DBIx::Recordset::CurrRow', $set1 ;


    printlog "ok\n";

    printlogf "SQLSelect All";
    print LOG "\n--------------------\n" ;

    $set1 -> SQLSelect ()  or die "not ok ($DBI::errstr)" ;

    Check ($TestIds[0], $TestFields[0], \@set1, undef, \%set1) or print "ok\n" ;

    #$^W = 1 ;

    DBIx::Recordset::Undef ('set1') ;

    # ---------------------

    printlogf "Setup Object for $Table[1]";
    print LOG "\n--------------------\n" ;

    $set2 = tie @set2, 'DBIx::Recordset', { '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[1]} or die "not ok ($DBI::errstr)" ;
    tie %set2, 'DBIx::Recordset::CurrRow', $set2 ;

    printlog "ok\n";

    printlogf "SQLSelect All";
    print LOG "\n--------------------\n" ;

    $set2 -> SQLSelect ()  or die "not ok ($DBI::errstr)" ;

    Check ($TestIds[1], $TestFields[1], \@set2, undef, \%set2) or print "ok\n" ;

    DBIx::Recordset::Undef ('set2') ;

    # ---------------------

    if ($Join)
        {
        printlogf "Setup Object for $Table[0], $Table[1]";
        print LOG "\n--------------------\n" ;

        $set3 = DBIx::Recordset->New ($DSN, "$Table[0], $Table[1]", $User, $Password) or die "not ok\n" ;
        tie @set3, 'DBIx::Recordset', $set3 ;
        tie %set3, 'DBIx::Recordset::CurrRow', $set3 ;

        printlog "ok\n";

        printlogf "SQLSelect All";
        print LOG "\n--------------------\n" ;

        if ($Driver eq 'mSQL')
            {
            my @f ;
            my $f ;
            my $fl ;
        
            foreach $fl (@{$TestFields[0]}) 
                {
                push @f, "$Table[0].$fl" ;
                }
            foreach $fl (@{$TestFields[1]}) 
                {
                push @f, "$Table[1].$fl" ;
                }
            $f = join (',', @f) ;

            $set3 -> SQLSelect ("$Table[0].id=$Table[1].id", $f)  or die "not ok ($DBI::errstr)" ;
            }
        else
            {    
            $set3 -> SQLSelect ("$Table[0].id=$Table[1].id")  or die "not ok ($DBI::errstr)" ;
            }

        Check ($TestIds[1], \@TestFields0_1, \@set3) or print "ok\n" ;

        DBIx::Recordset::Undef ('set3') ;

        # ---------------------

        printlogf "Setup Object for $Table[0], $Table[2]";
        print LOG "\n--------------------\n" ;

        $set4 = tie @set4, 'DBIx::Recordset', { '!DataSource'   =>  $DSN,
                                                '!Username'     =>  $User,
                                                '!Password'     =>  $Password,
                                                '!Table'        =>  "$Table[0], $Table[2]"} or die "not ok ($DBI::errstr)" ;

        tie %set4, 'DBIx::Recordset::CurrRow', $set4 ;

        printlog "ok\n";

        printlogf "SQLSelect All";
        print LOG "\n--------------------\n" ;

        if ($Driver eq 'mSQL')
            {        
            $set4 -> SQLSelect ("$Table[0].value1=$Table[2].value1", "$Table[0].id, $Table[0].name, $Table[2].txt")  or die "not ok ($DBI::errstr)" ;
            }
        else
            {
            $set4 -> SQLSelect ("$Table[0].value1=$Table[2].value1", "id, name, txt")  or die "not ok ($DBI::errstr)" ;
            }

        Check ($TestIds[0], ['id', 'name', 'txt'], \@set4) or print "ok\n" ;

        DBIx::Recordset::Undef ('set4') ;

        # ---------------------

        printlogf "Setup Object for $Table[0], $Table[3]";
        print LOG "\n--------------------\n" ;

        $set5 = DBIx::Recordset->New ($DSN, "$Table[0], $Table[3]", $User, $Password) or die "not ok\n" ;
        tie @set5, 'DBIx::Recordset', $set5 ;
        tie %set5, 'DBIx::Recordset::CurrRow', $set5 ;

        printlog "ok\n";

        printlogf "SQLSelect All";
        print LOG "\n--------------------\n" ;

        if ($Driver eq 'mSQL')
            {        
            $set5 -> Select ("$Table[0].id=$Table[3].id", "$Table[0].name, $Table[3].typ") or die "not ok ($DBI::errstr)" ;
            }
        else
            {
            $set5 -> Select ("$Table[0].id=$Table[3].id", "name, typ") or die "not ok ($DBI::errstr)" ;
            }

        Check ($TestIds[3], ['name', 'typ'], \@set5, 'typ') or print "ok\n" ;

        DBIx::Recordset::Undef ('set5') ;
        } # if ($Join)

    # ---------------------

    printlogf "Setup Object for $Table[0]";
    print LOG "\n--------------------\n" ;

    $set1 = tie @set1, 'DBIx::Recordset', { '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[0]} or die "not ok ($DBI::errstr)" ;
    tie %set1, 'DBIx::Recordset::CurrRow', $set1 ;

    printlog "ok\n";

    # ---------------------

    printlogf "Select id (where as hash)";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ({'id'=>2, '$operator'=>'='})  or die "not ok ($DBI::errstr)" ;

    Check ([2], $TestFields[0], \@set1, undef, \%set1) or print "ok\n" ;

    # ---------------------

    printlogf "Select id (where as string)";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ('id=4')  or die "not ok ($DBI::errstr)" ;

    Check ([4], $TestFields[0], \@set1) or print "ok\n" ;


    # ---------------------


    printlogf "Select name";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ({name => 'Third Name', '$operator'=>'='})  or die "not ok ($DBI::errstr)" ;

    Check ([3], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    if ($Join)
        {
        printlogf "Select $Table[0].name";
        print LOG "\n--------------------\n" ;

        $set1 -> Select ({"$Table[0].name" => 'Fourth Name', '$operator'=>'='})  or die "not ok ($DBI::errstr)" ;

        Check ([4], $TestFields[0], \@set1) or print "ok\n" ;

        # ---------------------

        printlogf "Select $Table[1].name2 id=id";
        print LOG "\n--------------------\n" ;

        *set1_ = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  "$Table[0], $Table[1]",
                                            '!Fields'       =>  "$Table[0].id, $Table[0].name, $Table[0].value1, $Table[0].addon",
                                           "'$Table[1].name2" => 'Second Name in Tab2',
                                           "\\$Table[0].id" => "$Table[1].id",
                                           '$operator'=>'='})  or die "not ok ($DBI::errstr)" ;

        Check ([2], $TestFields[0], \@set1_) or print "ok\n" ;

        # ---------------------

        printlogf "Select $Table[1].value2 id=id";
        print LOG "\n--------------------\n" ;

        $set1_ -> Select ({"\#$Table[1].value2" => '29993',
                               "\\$Table[0].id" => "$Table[1].id",
                               '$operator' => '='})  or die "not ok ($DBI::errstr)" ;


        Check ([3], $TestFields[0], \@set1_) or print "ok\n" ;

        DBIx::Recordset::Undef ('set1_') ;
        }

    # ---------------------

    printlogf "Select multiply values";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ({name => "Second Name\tFirst Name",
                           '$operator' => '='})  or die "not ok ($DBI::errstr)" ;


    Check ([1,2], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Select \$valuesplit";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ({value1 => "9991 9992\t9993",
                           '$valuesplit' => ' |\t',
                           '$operator' => '='})  or die "not ok ($DBI::errstr)" ;


    Check ([1,2,3], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Select multiply fields 1";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ({'+name&value1' => "9992",
                           '$operator' => '='})  or die "not ok ($DBI::errstr)" ;


    Check ([2], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Select multiply fields 2";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ({'+name&value1' => "Third Name",
                           '$operator' => '='})  or die "not ok ($DBI::errstr)" ;


    Check ([3], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Select multiply fields & values";
    print LOG "\n--------------------\n" ;

    $set1 -> Select ({'+name&value1' => "Second Name\t9991",
                           '$operator' => '='})  or die "not ok ($DBI::errstr)" ;


    Check ([1,2], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Search";
    print LOG "\n--------------------\n" ;

    $set1 -> Search ({id => 1,name => 'First Name',addon => 'Is'})  or die "not ok ($DBI::errstr)" ;


    Check ([1], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Search first two";
    print LOG "\n--------------------\n" ;

    $set1 -> Search ({'$start'=>0,'$max'=>2, '$order'=>'id'})  or die "not ok ($DBI::errstr)" ;


    Check ([1,2], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Search next ones";
    print LOG "\n--------------------\n" ;

    $set1 -> Search ({'$start'=>0,'$max'=>2, '$next'=>1, '$order'=>'id'})  or die "not ok ($DBI::errstr)" ;

    Check ([3,4], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------

    printlogf "Search prevs one";
    print LOG "\n--------------------\n" ;

    $set1 -> Search ({'$start'=>2,'$max'=>1, '$prev'=>1, '$order'=>'id'})  or die "not ok ($DBI::errstr)" ;

    Check ([2], $TestFields[0], \@set1) or print "ok\n" ;

    # ---------------------


    printlogf "Search last ones";
    print LOG "\n--------------------\n" ;

    $set1 -> Search ({'$start'=>5,'$max'=>5, '$next'=>1, '$order'=>'id'})  or die "not ok ($DBI::errstr)" ;

    Check ([11, 12], $TestFields[0], \@set1) or print "ok\n" ;

    DBIx::Recordset::Undef ('set1') ;

    # ---------------------
    if ($Join)
        {
        my $t0 ;
        my $t2 ;
 
        if ($Driver eq 'mSQL')
            {
            $t0 = "$Table[0]." ;
            $t2 = "$Table[2]." ;
            }
        else
            {
            $t0 = '' ;
            $t2 = '' ;
            }

        printlogf "New Search";
        print LOG "\n--------------------\n" ;

        *set6 = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  "$Table[0], $Table[2]",
                                            '!TabRelation'  =>  "$Table[0].value1=$Table[2].value1",
                                            '!Fields'       =>  "$t0\lid, $t0\lname, $t2\ltxt",
                                            "$t0\lid"       =>  "2\t4" }) or die "not ok ($DBI::errstr)" ;

        Check ([2,4], ['id', 'name', 'txt'], \@set6) or print "ok\n" ;

        # ---------------------

        printlogf "Search cont";
        print LOG "\n--------------------\n" ;

        $set6 -> Search ({"$t0\lname"            =>  "Fourth Name" }) or die "not ok ($DBI::errstr)" ;

        Check ([4], ['id', 'name', 'txt'], \@set6) or print "ok\n" ;

        # ---------------------

        printlogf "Search \$operator <";
        print LOG "\n--------------------\n" ;

        $set6 -> Search ({"$t0\lid"       =>  3,
                          '$operator'     =>  '<' }) or die "not ok ($DBI::errstr)" ;

        Check ([1,2], ['id', 'name', 'txt'], \@set6) or print "ok\n" ;

        # ---------------------

        printlogf "Search *id *name";
        print LOG "\n--------------------\n" ;

        $set6 -> Search ({"$t0\lid"            =>  4,
                          "$t0\lname"          =>  'Second Name',
                          "\*$t0\lid"           =>  '<',
                          "\*$t0\lname"         =>  '<>' }) or die "not ok ($DBI::errstr)" ;

        Check ([1,3], ['id', 'name', 'txt'], \@set6) or print "ok\n" ;

        # ---------------------

        printlogf "Search \$conj or";
        print LOG "\n--------------------\n" ;

        $set6 -> Search ({"$t0\lid"            =>  2,
                          "$t0\lname"          =>  'Fourth Name',
                          "\*$t0\lid"           =>  '<',
                          "\*$t0\lname"         =>  '=',
                          '$conj'              =>  'or' }) or die "not ok ($DBI::errstr)" ;

        Check ([1,4], ['id', 'name', 'txt'], \@set6) or print "ok\n" ;

        # ---------------------


        printlogf "Search multfield *<field>";
        print LOG "\n--------------------\n" ;

        $set6 -> Search ({"+$t0\lid|$t0\laddon" =>  "7\tit",
                          "$t0\lname"           =>  'Fourth Name',
                          "\*$t0\lid"            =>  '<',
                          "\*$t0\laddon"         =>  '=',
                          "\*$t0\lname"          =>  '<>',
                          '$conj'               =>  'and' }) or die "not ok ($DBI::errstr)" ;

        Check ([1,2,3,5,6,10], ['id', 'name', 'txt'], \@set6) or print "ok\n" ;

        # ---------------------



        printlogf "Search \$compconj";
        print LOG "\n--------------------\n" ;

        $set6 -> Search ({"+$t0\lid|$t0\laddon"     =>  "6\tit",
                          "$t0\lname"          =>  'Fourth Name',
                          "\*$t0\lid"           =>  '>',
                          "\*$t0\laddon"        =>  '<>',
                          "\*$t0\lname"         =>  '=',
                          '$compconj'     =>  'and',
                          '$conj'         =>  'or' }) or die "not ok ($DBI::errstr)" ;

        Check ([1,3,4,5,7,8,9,10,11,12], ['id', 'name', 'txt'], \@set6) or print "ok\n" ;


        DBIx::Recordset::Undef ('set6') ;

        # ---------------------

        printlogf "New Search id_typ";
        print LOG "\n--------------------\n" ;

        *set7 = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  "$Table[0], $Table[3]",
                                            '!TabRelation'  =>  "$Table[0].id=$Table[3].id",
                                            '!Fields'       =>  "$Table[0].name, $Table[3].typ"}) or die "not ok ($DBI::errstr)" ;

        Check ($TestIds[3], ['name', 'typ'], \@set7, 'typ') or print "ok\n" ;

        DBIx::Recordset::Undef ('set7') ;
        }

    # ---------------------


    printlogf "New Setup";
    print LOG "\n--------------------\n" ;
    *set8 = DBIx::Recordset -> Setup  ({  '!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                        '!Table'        =>  "$Table[1]"}) or die "not ok ($DBI::errstr)" ;

    print "ok\n" ;

    printlogf "SQLInsert";
    print LOG "\n--------------------\n" ;

    my %h = ('id'    => 21,
          'name2' => 'sqlinsert id 21',
          'value2'=> 1021) ;

    $set8 -> SQLInsert ('id, name2, value2', "21, 'sqlinsert id 21', 1021") or die "not ok ($DBI::errstr)" ;
    AddTestRowAndId (1, \%h) ;

    $set8 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set8) or print "ok\n" ;
    
    DBIx::Recordset::Undef ('set8') ;

    # ---------------------

    printlogf "New Insert";
    print LOG "\n--------------------\n" ;

    %h = ('id'    => 22,
          'name2' => 'sqlinsert id 22',
          'value2'=> 1022) ;


    *set9 = DBIx::Recordset -> Insert ({%h,
                                        ('!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                         '!Table'        =>  "$Table[1]")}) or die "not ok ($DBI::errstr)" ;
    AddTestRowAndId (1, \%h) ;

    $set9 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set9) or print "ok\n" ;
    
    # ---------------------

    printlogf "Update";
    print LOG "\n--------------------\n" ;

    %h = ('id'    => 22,
          'name2' => 'sqlinsert id 22u',
          'value2'=> 2022) ;


    $set9 -> Update (\%h, 'id=22') or die "not ok ($DBI::errstr)" ;
    AddTestRowAndId (1, \%h) ;

    $set9 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set9) or print "ok\n" ;
    
    DBIx::Recordset::Undef ('set9') ;


    # ---------------------

    printlogf "New Update";
    print LOG "\n--------------------\n" ;

    %h = ('id'    => 21,
          'name2' => 'sqlinsert id 21u',
          'value2'=> 2021) ;


    {
    local *set10 = DBIx::Recordset -> Update ({%h,
                                        ('!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                         '!Table'        =>  "$Table[1]",
                                         '!PrimKey'      =>  'id')}) or die "not ok ($DBI::errstr)" ;
    AddTestRowAndId (1, \%h) ;

    $set10 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set10) or print "ok\n" ;
    
    }
    # We use closing block instead of Undef here
    #DBIx::Recordset::Undef ('set10') ;

    # ---------------------


    printlogf "New Delete";
    print LOG "\n--------------------\n" ;

    %h = ('id'    => 21,
          'name2' => 'ssdadadqlid 21u',
          'value2'=> 202331) ;


    *set11 = DBIx::Recordset -> Delete ({%h,
                                        ('!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                         '!Table'        =>  "$Table[1]",
                                         '!PrimKey'      =>  'id')}) or die "not ok ($DBI::errstr)" ;
    DelTestRowAndId (1, 21) ;

    $set11 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set11) or print "ok\n" ;
    

    DBIx::Recordset::Undef ('set11') ;


    # ---------------------

    printlogf "New Execute Search (default)";
    print LOG "\n--------------------\n" ;


    *set12 = DBIx::Recordset -> Execute ({'id'  => 20,
                                       '*id' => '<',
                                       '!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                       '!Table'        =>  "$Table[1]",
                                       '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;

    Check ([1, 2, 3, 4], $TestFields[1], \@set12) or print "ok\n" ;
    

    # ---------------------

    printlogf "Execute =search";
    print LOG "\n--------------------\n" ;


    *set13 = DBIx::Recordset -> Execute ({'=search' => 'ok',
                        'name'  => 'Fourth Name',
                        '!DataSource'   =>  $DSN,
                        '!Username'     =>  $User,
                        '!Password'     =>  $Password,
                        '!Table'        =>  "$Table[0]",
                        '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;

    Check ([4], $TestFields[0], \@set13) or print "ok\n" ;
    
    DBIx::Recordset::Undef ('set13') ;

    # ---------------------

    printlogf "Execute =insert";
    print LOG "\n--------------------\n" ;


    $set12 -> Execute ({'=insert' => 'ok',
                        'id'     => 31,
                        'name2'  => 'insert by exec',
                        'value2'  => 3031,
    # Execute should ignore the following params, since it is already setup
                        '!DataSource'   =>  $DSN,
                        '!Username'     =>  $User,
                        '!Password'     =>  $Password,
                        '!Table'        =>  "quztr",
                        '!PrimKey'      =>  'id99'}) or die "not ok ($DBI::errstr)" ;

    AddTestRowAndId (1, {
                        'id'     => 31,
                        'name2'  => 'insert by exec',
                        'value2'  => 3031,
                        }) ;

    $set12 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set12) or print "ok\n" ;
    
    # ---------------------

    printlogf "Execute =update";
    print LOG "\n--------------------\n" ;


    $set12 -> Execute ({'=update' => 'ok',
                        'id'     => 31,
                        'name2'  => 'update by exec'}) or die "not ok ($DBI::errstr)" ;

    AddTestRowAndId (1, {
                        'id'     => 31,
                        'name2'  => 'update by exec',
                        }) ;

    $set12 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set12) or print "ok\n" ;
    
    # ---------------------

    printlogf "Execute =insert";
    print LOG "\n--------------------\n" ;

    $set12 -> Execute ({'=insert' => 'ok',
                        'id'     => 32,
                        'name2'  => 'insert/upd by exec',
                        'value2'  => 3032}) or die "not ok ($DBI::errstr)" ;

    AddTestRowAndId (1, {
                        'id'     => 32,
                        'name2'  => 'insert/upd by exec',
                        'value2'  => 3032,
                        }) ;

    $set12 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set12) or print "ok\n" ;
    

    # ---------------------
    #
    #printlogf "Execute =update =insert 2";
    #print LOG "\n--------------------\n" ;
    #
    #$set12 -> Execute ({'=insert' => 'ok',
    #                    '=update' => 'ok',
    #                    'id'     => 32,
    #                    'name2'  => 'ins/update by exec',
    #                   'value2'  => 3032}) or die "not ok ($DBI::errstr)" ;
    #
    #AddTestRowAndId (1, {
    #                    'id'     => 32,
    #                    'name2'  => 'ins/update by exec',
    #                    'value2'  => 3032,
    #                    }) ;
    #
    #$set12 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    #Check ($TestIds[1], $TestFields[1], \@set12) or print "ok\n" ;
    #    
    # ---------------------

    printlogf "Execute =delete";
    print LOG "\n--------------------\n" ;

    $set12 -> Execute ({'=delete' => 'ok',
                        'id'     => 32,
                        'name2'  => 'ins/update by exec',
                        'value2'  => 3032}) or die "not ok ($DBI::errstr)" ;

    DelTestRowAndId (1, 32) ;

    $set12 -> SQLSelect ()  or die "not ok in SELECT ($DBI::errstr)" ;
    Check ($TestIds[1], $TestFields[1], \@set12) or print "ok\n" ;
    


    DBIx::Recordset::Undef ('set12') ;



    # ---------------------

    printlogf "Array Update/Insert";
    print LOG "\n--------------------\n" ;

    *set20 = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[0],
                                            '$order'        =>  'id',
                                            '!PrimKey'      =>  'id',
                                            'id'            =>  7,
                                            '*id'           =>  '<' }) or die "not ok ($DBI::errstr)" ;


    Check ([1,2,3,4,5,6], $TestFields[0], \@set20) or print "ok\n" ;

    $set20[3]{name} = 'New Name on id 4' ;
    $set20[3]{value1} = 4444 ;
        
    AddTestRowAndId (0, {
                        'id'   => 4,
                        'name' => 'New Name on id 4',
                        'value1' => 4444
                        }) ;

    $set20[7]{id}    = 1234 ;
    $set20[7]{name}  = 'New rec' ;

    AddTestRowAndId (0, {
                        'id'   => 1234,
                        'name' => 'New rec',
                        }) ;

    $set20  -> Search ({'id'            =>  4}) or die "not ok ($DBI::errstr)" ;

    Check ([4], $TestFields[0], \@set20) or print "ok\n" ;
    
    $set20  -> Search ({'id'            =>  1234}) or die "not ok ($DBI::errstr)" ;

    Check ([1234], $TestFields[0], \@set20) or print "ok\n" ;
    

    # ---------------------

    printlogf "Array Update/Insert -> Flush";
    print LOG "\n--------------------\n" ;

    $set20[0]{id}    = 1234 ;
    $set20[0]{name}  = 'New rec 1234' ;

    *set20c = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[0],
                                            '$order'        =>  'id',
                                            '!PrimKey'      =>  'id',
                                            'id'            =>  1234}) or die "not ok ($DBI::errstr)" ;

    Check ([1234], $TestFields[0], \@set20c) or print "ok\n" ;

    
    
    # write it to the db
    print LOG "Flush\n" ;
    $set20 -> Flush () ;
    
    AddTestRowAndId (0, {
                        'id'   => 1234,
                        'name' => 'New rec 1234',
                        }) ;
    
    
    #$set20c -> Search ({'id'            =>  1234}) or die "not ok ($DBI::errstr)" ;
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    DBIx::Recordset::Undef ('set20c') ;
    *set20c = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[0],
                                            '!PrimKey'      =>  'id',
                                            'id'            =>  1234}) or die "not ok ($DBI::errstr)" ;
    
    Check ([1234], $TestFields[0], \@set20c) or print "ok\n" ;
    
    
    printlogf "Array Insert Hashref";
    print LOG "\n--------------------\n" ;

    $set20[8] = {id => 12345, 'name' => 'New rec 12345'}  ;

    # write it to the db
    print LOG "Flush\n" ;
    $set20 -> Flush () ;
    
    AddTestRowAndId (0, {
                        'id'   => 12345,
                        'name' => 'New rec 12345',
                        }) ;
    
    
    #$set20c -> Search ({'id'            =>  12345}) or die "not ok ($DBI::errstr)" ;
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    # we try here undef instead of DBIx::Recordset::Undef ('set20c') ;
    undef *set20c ;
    
    
    *set20c = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[0],
                                            '!PrimKey'      =>  'id',
                                            'id'            =>  12345}) or die "not ok ($DBI::errstr)" ;
    
    Check ([12345], $TestFields[0], \@set20c) or print "ok\n" ;
    
    printlogf "Array Add Record";
    print LOG "\n--------------------\n" ;
    
    $set20 -> Add ({id => 123456, 'name' => 'New rec 123456'})  ;
    
    # write it to the db
    print LOG "Flush\n" ;
    $set20 -> Flush () ;
    
    AddTestRowAndId (0, {
                        'id'   => 123456,
                        'name' => 'New rec 123456',
                        }) ;
    
    
    #$set20c -> Search ({'id'            =>  123456}) or die "not ok ($DBI::errstr)" ;
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    DBIx::Recordset::Undef ('set20c') ;
    *set20c = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[0],
                                            '!PrimKey'      =>  'id',
                                            'id'            =>  123456}) or die "not ok ($DBI::errstr)" ;
    
    Check ([123456], $TestFields[0], \@set20c) or print "ok\n" ;
    
    printlogf "Array Add Empty Record";
    print LOG "\n--------------------\n" ;
    
    my $ndx = $set20 -> Add ()  ;
    
    $set20[$ndx]{id} = 1234567 ;
    $set20[$ndx]{name}  = 'New rec 1234567' ;

    
    # write it to the db
    print LOG "Flush\n" ;
    $set20 -> Flush () ;
    
    AddTestRowAndId (0, {
                        'id'   => 1234567,
                        'name' => 'New rec 1234567',
                        }) ;
    
    
    #$set20c -> Search ({'id'            =>  1234567}) or die "not ok ($DBI::errstr)" ;
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    DBIx::Recordset::Undef ('set20c') ;
    *set20c = DBIx::Recordset -> Search ({  '!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  $Table[0],
                                            '!PrimKey'      =>  'id',
                                            'id'            =>  1234567}) or die "not ok ($DBI::errstr)" ;
    
    Check ([1234567], $TestFields[0], \@set20c) or print "ok\n" ;
    
    DBIx::Recordset::Undef ('set20') ;
    DBIx::Recordset::Undef ('set20c') ;

    

        {
        local *set13 = DBIx::Recordset -> Setup ({'!DataSource'   =>  $DSN,
                                            '!Username'     =>  $User,
                                            '!Password'     =>  $Password,
                                            '!Table'        =>  "$Table[1]",
                                            '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;
    
        # ---------------------

        printlogf "Select id (Hash)";
        print LOG "\n--------------------\n" ;

        my %set13h ;

        tie %set13h, 'DBIx::Recordset::Hash', $set13 ;
    
        $set13h[0] = $set13h{2} ;


        Check ([2], $TestFields[1], \@set13h) or print "ok\n" ;
    
        # ---------------------

        printlogf "Iterate over ::Hash";
        print LOG "\n--------------------\n" ;
        #
            {
            my $i ;
            my $v ;
            my $k ;
            my $n ;
            my @set13h ;

            $i = 0 ;
            while (($k, $v) = each %set13h)
                {
                @set13h = () ;
                $set13h[0] = $v ;
                Check ([$k], $TestFields[1], \@set13h) or print "ok\n" ;
                $i++ ;
                }
    
            $n = ($#{$TestIds[1]})+1 ;
            if ($i != $n)
                {
                print "ERROR\n" ;
                print "Not enougth records (get $i, expected $n)\n" ;
                $errors++ ;         
                }
            }

        #untie %set13h ;
        #@set13h = () ;
        #DBIx::Recordset::Undef ('set13') ;
        }

    # ---------------------


    *set14 = DBIx::Recordset -> Setup ({'!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                        '!Table'        =>  "$Table[1]",
                                        '!HashAsRowKey' =>  1,
                                        '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;
    

    printlogf "Select id (HashAsRowKey)";
    print LOG "\n--------------------\n" ;

   
    my @set14h = () ;
    my @set15h = () ;
    $set14h[0] = $set14{3} ;

    Check ([3], $TestFields[1], \@set14h) or print "ok\n" ;
    
    @set14h = () ;
    @set15h = () ;

    # ---------------------


    printlogf "Select name (Hash) with setup";
    print LOG "\n--------------------\n" ;

    tie %set15h, 'DBIx::Recordset::Hash', {'!DataSource'   =>  $DSN,
                                           '!Username'     =>  $User,
                                           '!Password'     =>  $Password,
                                           '!Table'        =>  "$Table[1]",
                                           '!PrimKey'      =>  'name2'} or die "not ok ($DBI::errstr)" ;

    $set15h[0] = $set15h{'Fourth Name in Tab2'} ;

    Check ([4], $TestFields[1], \@set15h) or print "ok\n" ;
    

    # ---------------------

    printlogf "Modify Hash";
    print LOG "\n--------------------\n" ;

    $set15h{'Fourth Name in Tab2'}{value2} = 4444 ;

    tied (%set15h) -> Flush () ;

    AddTestRowAndId (1, {
                        'id'   => 4,
                        'value2' => 4444 ,
                        }) ;

    
    
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    DBIx::Recordset::Undef ('set14') ;
    *set14 = DBIx::Recordset -> Setup ({'!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                        '!Table'        =>  "$Table[1]",
                                        '!HashAsRowKey' =>  1,
                                        '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;
    $set14 -> Search ({'id' => 4})  ;
    


    Check ([4], $TestFields[1], \@set14) or print "ok\n" ;
    

    # ---------------------

    printlogf "Add To Hash";
    print LOG "\n--------------------\n" ;

    $set15h{'Fifth Name in Tab2'}{id} = 5 ;
    $set15h{'Fifth Name in Tab2'}{value2} = 5555 ;

    tied (%set15h) -> Flush () ;

    AddTestRowAndId (1, {
                        'id'   => 5,
                        'name2'=> 'Fifth Name in Tab2',
                        'value2' => 5555 ,
                        }) ;

    
    
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    DBIx::Recordset::Undef ('set14') ;
    *set14 = DBIx::Recordset -> Setup ({'!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                        '!Table'        =>  "$Table[1]",
                                        '!HashAsRowKey' =>  1,
                                        '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;
    $set14 -> Search ({'id' => 5})  ;

    Check ([5], $TestFields[1], \@set14) or print "ok\n" ;
    


    # ---------------------

    printlogf "Add Hashref To Hash ";
    print LOG "\n--------------------\n" ;

    $set15h{'Sixth Name in Tab2'}= {id => 6, value2 => 6666}  ;

    tied (%set15h) -> Flush () ;

    AddTestRowAndId (1, {
                        'id'   => 6,
                        'name2'=> 'Sixth Name in Tab2',
                        'value2' => 6666 ,
                        }) ;

    
    
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    DBIx::Recordset::Undef ('set14') ;
    *set14 = DBIx::Recordset -> Setup ({'!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                        '!Table'        =>  "$Table[1]",
                                        '!HashAsRowKey' =>  1,
                                        '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;
    $set14 -> Search ({'id' => 6})  ;

    Check ([6], $TestFields[1], \@set14) or print "ok\n" ;
    



    # ---------------------

    printlogf "Modify PrimKey in Hash";
    print LOG "\n--------------------\n" ;

    $set15h{'Fourth Name in Tab2'}{name2} = 'New Fourth Name' ;

    tied (%set15h) -> Flush () ;

    AddTestRowAndId (1, {
                        'id'   => 4,
                        'name2' => 'New Fourth Name' ,
                        }) ;

    
    
    # The resetup is neccessary to work with all, also stupid drivers (MSAccess)
    DBIx::Recordset::Undef ('set14') ;
    *set14 = DBIx::Recordset -> Setup ({'!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                        '!Table'        =>  "$Table[1]",
                                        '!HashAsRowKey' =>  1,
                                        '!PrimKey'      =>  'id'}) or die "not ok ($DBI::errstr)" ;
    $set14 -> Search ({'id' => 4})  ;
    


    Check ([4], $TestFields[1], \@set14) or print "ok\n" ;
    

    printlogf "Test Syntax error";
    print LOG "\n--------------------\n" ;


    $rc = $set14 -> Update ({id => 9999}, "qwer=!§" ) and die "not ok (returns $rc)" ;
    
    print "ok\n" if (!defined ($rc)) ;

    DBIx::Recordset::Undef ('set14') ;
    untie %set15h ;


    printlogf "Test error within setup";
    print LOG "\n--------------------\n" ;

    *set14 = DBIx::Recordset -> Update ({'!DataSource'   =>  $DSN,
                                        '!Username'     =>  $User,
                                        '!Password'     =>  $Password,
                                        '!Table'        =>  $Table[1],
                                        '!HashAsRowKey' =>  1,
                                        '!PrimKey'      =>  'id'},
                                        'qwert=!%&') ;

    die "not ok" if (defined ($set14)) ;
    print "ok\n" if (!defined ($set14)) ;

    DBIx::Recordset::Undef ('set14') ;

    #########################################################################################

    if ($errors)
        {
        print "\n$errors Errors detected for driver $Driver\n" ;
        }
    else
        {
        print "\nTests passed successfully for driver $Driver\n" ;
        }

    return $errors ;
    }

#########################################################################################


unlink "test.log" ;

if ($#ARGV != -1)
    {
    eval { do $configfile ; } ;


    $Driver      = $ARGV[0] ;
    $DSN         = $ARGV[1] || $Drivers{$Driver}{dsn} ; 
    $User        = $ARGV[2] || $Drivers{$Driver}{user}  ;
    $Password    = $ARGV[3] || $Drivers{$Driver}{pass}  ;

    $> = $Drivers{$Driver}{uid} if (defined ($Drivers{$Driver}{uid})) ;
    $rc = DoTest ($Driver, $DSN, $User, $Password) ;
    $> = $< if ($Drivers{$Driver}{uid}) ;
    
    $fatal = 0 ;

    exit $rc ;
    }

do $configfile ;

@drivers = sort keys %Drivers ;

foreach $drv (@drivers)
    {
    $> = $Drivers{$drv}{uid} if (defined ($Drivers{$drv}{uid})) ;
    $errcnt {$drv} = DoTest ($drv, $Drivers{$drv}{dsn}, $Drivers{$drv}{user}, $Drivers{$drv}{pass}) ;
    $> = $< if ($Drivers{$drv}{uid}) ;
    }
     
$err = 0 ;
print "\nSummary:\n" ;

foreach $drv (@drivers)
    {
    if ($errcnt {$drv})
        {
        print "$errcnt{$drv} Errors detected for $drv\n" ;
        }
    else
        {
        print "Tests for $drv passed successfully\n" ;
        }
    $err += $errcnt {$drv} ;
    }

if ($err)
    {
    print "\n$err Errors detected at all\n" ;
    }
else
    {
    print "\nAll tests passed successfully\n" ;
    }

$fatal = 0 ;

__END__
