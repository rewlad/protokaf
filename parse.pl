#!/usr/bin/perl
use strict;

my $mode = $ARGV[0]=~/^(intf|impl)$/ ? $1 : die;
my @lines = <STDIN>;
s{//.*}{} for @lines;
my $src = join ' ', @lines;

my $chk_unparsed = sub{ /\S/ and die "unparsed ($_)" for @_ };

my $add_message = sub{
    my(%message)=@_;
    $message{body}=~s{\b(repeated\s+)?([A-Z]\w*)\s+([a-z]\w*)\s*=\s*(0x[0-9a-fA-F]{4})\s*;}{
        my %msg = (repeated=>"$1",type=>"$2",name=>"$3",id=>"$4");
        $msg{serde} = 
            $msg{type} eq 'String' ? "com.squareup.wire.ProtoAdapter.STRING" :
            "$msg{type}Serde";
        push @{$message{fields}||=[]}, \%msg; 
        ""
    }egs;
    &$chk_unparsed(delete $message{body});
    print $mode eq 'intf' ? do{
        my $args = join ', ', map{ 
            $$_{repeated} ? "$$_{name}: Seq[$$_{type}] = Nil" : "$$_{name}: Option[$$_{type}] = None" 
        } @{$message{fields}||[]};
        "case class $message{name}($args)\n" #(val encode: $message{name}=>Array[Byte])\n"
    } : $mode eq 'impl' ? do{
        my $size = join '', map{
            "\n                value.$$_{name}.foreach(item => res += $$_{serde}.encodedSizeWithTag($$_{id}, item))"
        } @{$message{fields}||[]};
        my $encode = join '', map{
            "\n                value.$$_{name}.foreach(item => $$_{serde}.encodeWithTag(writer, $$_{id}, item))"
        } @{$message{fields}||[]};
        my $nil = join '', map{
            $$_{repeated} ? 
                "\n                var prep_$$_{name}: List[$$_{type}] = Nil" : 
                "\n                var prep_$$_{name}: Option[$$_{type}] = None"
        } @{$message{fields}||[]};
        my $case = join '', map{
            $$_{repeated} ? 
                "\n                    case $$_{id} => prep_$$_{name} = $$_{serde}.decode(reader) :: prep_$$_{name}" :
                "\n                    case $$_{id} => prep_$$_{name} = Option($$_{serde}.decode(reader))"
        } @{$message{fields}||[]};
        my $args = join ', ', map{ 
            "prep_$$_{name}"
        } @{$message{fields}||[]};
        "
        object $message{name}Serde extends com.squareup.wire.ProtoAdapter[$message{name}](com.squareup.wire.FieldEncoding.LENGTH_DELIMITED, classOf[$message{name}]) {
            override def encodedSize(value: $message{name}): Int = {
                var res = 0
                $size
                return res
            }
            override def encode(writer: com.squareup.wire.ProtoWriter, value: $message{name}) = {
                $encode
            }
            override def decode(reader: com.squareup.wire.ProtoReader) = {
                $nil
                var done = false
                while(!done) reader.nextTag() match {
                    case -1 => done = true
                    $case
                    case _ => reader.peekFieldEncoding.rawProtoAdapter.decode(reader)
                }
                $message{name}($args)
            }
        }"
    } : die;
};

$src=~s{\bmessage\s+([A-Z]\w*)\s+\{(.*?)\}\s*}{ &$add_message(name=>"$1",body=>"$2"); ""}egs;
&$chk_unparsed($src);

=sk
Duration
Instant
Boolean
Decimal
=cut
