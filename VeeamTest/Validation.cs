using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.Devices;

namespace VeeamTest
{
    class Validation
    {
        public static void StringReadValidation ( string [] args )
        {

            if ( args.Length == 0 || args.Length > 3 )
            {
                throw new Exception( "Please enter arguments up to the following pattern:" +
                                     "\n compress(decompress) [\"Source file.extension\"] [\"Destination file.extension\"]." );
            }

            if ( args [ 0 ].ToLower() != "compress" && args [ 0 ].ToLower() != "decompress" )
            {
                throw new Exception( "First argument must be \"compress\" or \"decompress\"." );
            }

            if ( args [ 1 ].Length == 0 )
            {
                throw new Exception( "No source file name was specified." );
            }

            if ( !File.Exists( args [ 1 ] ) )
            {
                throw new Exception( "No source file was found." );
            }


            FileInfo _fileIn = new FileInfo(args[1]);
            FileInfo _fileOut = new FileInfo(args[2]);

            if ( args [ 1 ] == args [ 2 ] )
            {
                throw new Exception( "Source and destination files shall be different." );
            }

            if ( _fileIn.Extension == ".gz" && args [ 0 ] == "compress" )
            {
                throw new Exception( "File has already been compressed." );
            }

            if ( /*_fileOut.Extension == ".gz" &&*/ _fileOut.Exists )
            {
                Console.WriteLine( "Destination file already exists. Do you want override? Y/N " );

                while ( true)
                {
                    if(Console.ReadKey( true ).Key == ConsoleKey.Y  )
                    {
                        //Console.WriteLine( "Overriding" );
                        break;
                    }
                    if ( Console.ReadKey( true ).Key == ConsoleKey.N )
                    {
                        throw new Exception( "Destination file already exists. Please indiciate the different file name." );
                        //Environment.Exit( 0 );
                    }
                }

            }

            if ( _fileIn.Extension != ".gz" && args [ 0 ] == "decompress" )
            {
                throw new Exception( "File to be decompressed shall have .gz extension." );
            }
            if ( _fileIn.Length < 1 )
            {
                throw new Exception( "Source file is empty" );
            }

            if ( args [ 2 ].Length == 0 )
            {
                throw new Exception( "No destination file name was specified." );
            }
        }


        public static void HardwareValidation(ulong perThreadmemoryUsage, ulong threadCount )
        {
            ulong minFreeMemory =  (threadCount * perThreadmemoryUsage/*125*(1024*1024)*/) ;
            //Console.WriteLine( "MinFreeMemory = " + minFreeMemory / ( 1024 * 1024 ) );
            if ( new ComputerInfo().AvailableVirtualMemory < minFreeMemory )
            {
                throw new Exception( "This program requires at least " + ( minFreeMemory / ( 1024 * 1024 ) ) + " mb of free RAM" );
            }
        }
    }
}
