using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace VeeamTest
{
    class Program
    {
        static void Main ( string [] args )
        {
            foreach ( string arg in args )
            {
                Console.WriteLine( arg );
            }
            ChooseAction( ref args );

        }



        static void ChooseAction( ref string[] args )
        {
            if(args.Length < 1) return;

            if ( args [ 0 ].Equals( "decompress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                Decompress();
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                Compress();
            }
        }


        static void Compress()
        {
            Console.WriteLine("Compress");
            Console.ReadKey();
        }

        static void Decompress()
        {
            Console.WriteLine( "DECompress" );
            Console.ReadKey();
        }
    }
}
