
###################################################################################
#
#   DBIx::Compat - Copyright (c) 1997-1998 Gerald Richter / ECOS
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file.
#   For use with Apache httpd and mod_perl, see also Apache copyright.
#
#   THIS IS BETA SOFTWARE!
#
#   THIS PACKAGE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR
#   IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
#   WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
###################################################################################

package DBIx::Compat ;

use DBI ;

sub SelectFields

    {
    my $hdl   = shift ; 
    my $table = shift ; 
    
    my $sth = $hdl -> prepare ("select * from $table where 1=0") ;
    $sth -> execute () or return undef ;
    
    return $sth ;
    }

sub SelectAllFields

    {
    my $hdl   = shift ; 
    my $table = shift ; 
    
    my $sth = $hdl -> prepare ("select * from $table") ;
    $sth -> execute () or return undef ;
    
    return $sth ;
    }



sub ListFields

    {
    my $hdl   = shift ; 
    my $table = shift ; 
    
    my $sth = $hdl -> prepare ("LISTFIELDS $table") ;
    $sth -> execute () or return undef ;

    # Meaning of TYPE has changed from 1.19_18 -> 1.19_19
    $Compat{mSQL}{QuoteTypes} = {   1=>1,   12=>1,   -1=>1 } if (exists ($sth -> {msql_type})) ;

    return $sth ;
    }

sub ListFieldsFunc

    {
    my $hdl   = shift ; 
    my $table = shift ; 
    
    my $sth = $hdl -> func($table, 'listfields' )  or return undef ;
    
    return $sth ;
    }


sub ListTables

    {
    my $hdl   = shift ; 

    return $hdl -> tables ;
    }

sub ListTablesFunc

    {
    my $hdl   = shift ; 

    my @tabs = $hdl -> tables ;
    
    # try the _ListTables function for DBD::mysql before 1.21..
    @tabs = $hdl -> func('_ListTables' ) if ($#tabs < 0) ;

    return @tabs ;
    }

sub ListTablesPg

    {
    my $hdl   = shift ; 
    my @tabs ;
    my $st = $hdl -> tables ;

    while ($dat = $st -> fetch)
        {
        push @tabs, $dat -> [0] ;
        }	

    return @tabs ;
    }

####################################################################################


%Compat =
    (
    '*'  =>
            {
            'Placeholders'   => 10,              # Default: Placeholder are supported
            'ListFields'     => \&SelectFields,  # Default: Use Select to get field names
            'ListTables'     => \&ListTables,    # Default: Use DBI $dbh -> tables
            'QuoteTypes'     => { },             # Default: Don't know what to quote
            'SupportJoin'    => 1,               # Default: Driver supports joins (select with multiple tables)
            'SupportSQLJoin' => 1,               # Default: Driver supports INNER/LEFT/RIGHT JOIN Syntax in SQL select
            'SQLJoinOnly2Tabs' => 1,             # Default: Driver supports LEFT/RIGHT JOIN only with two tables
            'HaveTypes'      => 1,               # Default: Driver supports $sth -> {TYPE}
            'NullOperator'   => 'IS',            # Default: Operator to compare with NULL is IS
	    'NeedNullInCreate' => '',            # Default: NULL allowed without explicit declare in CREATE
	    'EmptyIsNull'    => 0,		 # Default: Empty strings ('') and NULL are different
             },

    'ConfFile' =>
	{
            'Placeholders' => 2,		# Placeholders supported, but the perl
						#   type must be the same as the db type
            'ListFields'     => \&SelectFields,     
            'SupportJoin'    => 0,
            'HaveTypes'      => 0		# Driver does not support $sth -> {TYPE}
	},	

    'CSV' =>
	{
            'Placeholders' => 2,                # Placeholders supported, but the perl
						#   type must be the same as the db type
            'ListFields'     => \&SelectFields,      
            'SupportJoin'    => 0,
            'SupportSQLJoin'	=> 0,		    # Driver does not supports INNER/LEFT/RIGHt JOIN Syntax in SQL select
            'HaveTypes'      => 0,              # Driver does not support $sth -> {TYPE}
            'ListTables'     => undef,		# no tables
	    'EmptyIsNull'    => 1,		# DBD::CSV does not really knows about NULL
	},	

    'XBase' =>
	{
          #  'Placeholders' => 2,               # Placeholders supported, but the perl
						#   type must be the same as the db type
            'ListFields'     => \&SelectAllFields,      
            'SupportJoin'    => 0,
            'HaveTypes'      => 0               # Driver does not support $sth -> {TYPE}
	},	

    'Pg' => {
            'Placeholders' => 2,                # Placeholders supported, but the perl
						#   type must be the same as the db type

            'SupportSQLJoin' => 0,              # Driver does not supports INNER/LEFT/RIGHt JOIN Syntax in SQL select
            'QuoteTypes' =>
                {   17=>1,   18=>1,   19=>1,   20=>1,   25=>1,  409=>1,  410=>1,
                   411=>1,  605=>1, 1002=>1, 1003=>1, 1004=>1, 1009=>1, 1026=>1,
                  1039=>1, 1040=>1, 1041=>1, 1042=>1, 1043=>1 },
#### Use the following line for older DBD::Pg versions (< 0.89) which does
#    not support the table_info function
#            'ListTables'     => \&ListTablesPg,    # DBD::Pg
            },
    
    'mSQL' => {
            'Placeholders'	=> 2,		    # Placeholders supported, but the perl
						    #   type must be the same as the db type
            'SupportSQLJoin'	=> 0,		    # Driver does not supports INNER/LEFT/RIGHt JOIN Syntax in SQL select
            'ListFields'	=> \&ListFields,    # mSQL has it own ListFields function
            'ListTables'	=> \&ListTablesFunc,# DBD::mysql $dbh -> func
            'NullOperator'	=> '=',		    # Operator to compare with NULL is =
            'QuoteTypes'	=> {   1=>1,   12=>1,   -1=>1 }
# ### use the following line for older mSQL drivers
#            'QuoteTypes'	=> {   2=>1,   6=>1 }
            },

    'mysql' => {
            'ListFields'   => \&ListFields,	    # mysql has it own ListFields function
            'QuoteTypes'   => {   1=>1,   12=>1,   -1=>1 , 9=>1},
            'Placeholders' => 10,		    # Placeholders supported, but the perl
						    #   type must be the same as the db type
            'SQLJoinOnly2Tabs' => 0,		    # mysql supports LEFT/RIGHT JOIN with more than two tables
            'ListTables'     => \&ListTablesFunc,   # DBD::mysql $dbh -> func
            },

    'Solid' => {
            'Placeholders' => 3,		    # Placeholders supported, but cannot use a
						    #   string where a number expected
            'QuoteTypes'   => {   1=>1,   12=>1,   -1=>1 }
            },

    'ODBC' => {
            'Placeholders' => 1,		    # Placeholders supported, but seems not
						    #   to works all the time ?
            'QuoteTypes'   => {   1=>1,   12=>1,   -1=>1}
            },
    'Oracle' => {
            'Placeholders' => 3,		    # Placeholders supported, but cannot use a
						    #   string where a number expected
            'QuoteTypes'   => {   1=>1,   12=>1,   -1=>1 },
            'SupportSQLJoin' => 3,		    # Oracle need  a = b (+) instead of  INNER/LEFT/RIGHt JOIN Syntax in SQL select
	    'EmptyIsNull'    => 1,		    # Oracle converts empty strings ('') to NULL
# older DBD::Orcales only need the following one entry, but some test may fail
#		'HaveTypes'      => 0		    #  Driver does not supports $sth -> {TYPE}
            },

    'Sybase'  =>
            {
            'Placeholders'   => 10,
            'ListFields'     => \&SelectFields,  
            'QuoteTypes'     => {
                -6=>0, -4=>0, -2=>0, -1=>1, 1=>1, 2=>0, 3=>1,
                4=>0, 6=>0, 7=>0, 9=>0 

				},
            'SupportSQLJoin' => 2,               # Driver need *= instead of  INNER/LEFT/RIGHt JOIN Syntax in SQL select
#            'SupportSQLJoin' => 0, 
            'HaveTypes'      => 1,       
            'NullOperator'   => 'IS',
	    'NeedNullInCreate' => 'NULL'          
	},
    ) ;    


###########################################################################################################

sub GetItem

    {
    my ($driver, $name) = @_ ;

    return $Compat{$driver}{$name}  if (exists ($Compat{$driver}{$name})) ;
    return $Compat{'*'}{$name} ; 
    }

1 ;


=head1 NAME

DBIx::Compat - Perl extension for Compatibility Infos about DBD Drivers

=head1 SYNOPSIS

  use DBIx::Compat;

  $DBIx::Compat::Compat{$Driver}{Placeholders}  
  $DBIx::Compat::Compat{$Driver}{QuoteTypes}  
  $DBIx::Compat::Compat{$Driver}{ListFields}  


=head1 DESCRIPTION

DBIx::Compat contains a hash which gives information about DBD drivers, to allow
to write driver independent programs.

Currently there are three attributes defined:

=head2 B<Placeholders>

Gives information if and how placeholders are supported:

=over 4

=item B<0> = Not supported

=item B<1> = Supported, but not fully, unknown how much

=item B<2> = Supported, but perl type must be the same as type in db

=item B<3> = Supported, but can not give a string when a numeric type is in the db

=item B<10> = Supported under all circumstances

=back


=head2 B<QuoteTypes>

Gives information which datatypes must be quoted when passed literal
(not via a placeholder). Contains a hash with all type number which
need to be quoted.

  $DBIx::Compat::Compat{$Driver}{QuoteTypes}{$Type} 
  
will be true when the type in $Type for the driver $Driver must be
quoted.

=head2 B<ListFields>

A function which will return information about all fields of an table.
Needs an database handle and a tablename as argument.
Must at least return the fieldnames and the fieldtypes.

 Example:
  
  $ListFields = $DBIx::Compat::Compat{$Driver}{ListFields} ;
  $sth = &{$ListFields}($DBHandle, $Table) or die "Cannot list fields" ;
    
  @{ $sth -> {NAME} } ; # array of fieldnames
  @{ $sth -> {TYPE} } ; # array of filedtypes

  $sth -> finish ;

=head1 Supported Drivers

Currently there are entry for

=item B<DBD::mSQL>

=item B<DBD::mysql>

=item B<DBD::Pg>

=item B<DBD::Solid>

=item B<DBD::ODBC>

=item B<DBD::CSV>

=item B<DBD::Oracle>

if you detect an error in the definition or add an definition for a new
DBD driver, please mail it to the author.


=head1 AUTHOR

G.Richter <richter*dev.ecos.de>

=head1 SEE ALSO

perl(1), DBI(3), DBIx::Recordset(3)

=cut
