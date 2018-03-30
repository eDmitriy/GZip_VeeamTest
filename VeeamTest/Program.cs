using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;


namespace VeeamTest
{
    class Program
    {
        #region Vars

        public static int threadCount = Environment.ProcessorCount;
        public static readonly long BufferSize = 1024*1024;


        public static DataBlock[] dataBlocks = new DataBlock[0];
        //public static ulong dataBlocksBufferedMemoryAmount = 0;
        //public static ulong maxMemoryForDataBlocksBuffer = 1024 * 1024 * 100; //100 mb

        //public static List<long> headersFound = new List< long >();



        static DateTime startTime;
        private static string currOperation = "";
        public static bool readEnded;

        public static Writer _outFileWriter = null;

        #endregion



        static void Main ( string [] args )
        {
            Console.CancelKeyPress += new ConsoleCancelEventHandler( CancelKeyPress );
            ShowInfo();

            try
            {
                #region Validation

                Validation.HardwareValidation();
                Validation.StringReadValidation( args );

                #endregion
                
                startTime = DateTime.Now;
                CreateWorkers(  args );
            }

            catch ( Exception ex )
            {
                Console.WriteLine( "Error is occured!\n Method: {0}\n Error description: {1}", ex.TargetSite, ex.Message );
                //return 1;
            }


            #region Result

            Console.WriteLine( string.Format( "\nCompleted in {0} seconds", ( DateTime.Now - startTime ).TotalSeconds ) );
            Console.WriteLine( "\nHeaders found = " + DecompressReader.headersFound.Count );
            Console.WriteLine( "Writes count = " + Writer.writedIndexes.Count );

            #endregion

            Console.WriteLine("\nPress any key to exit!" );
            Console.ReadKey();
        }

        
        static int CreateWorkers( string[] args )
        {
            currOperation = args[ 0 ].ToLower();
            Console.WriteLine( "\n\n"+currOperation + "ion of " + args [ 1 ] + " started..." );


            #region Create Reader

            Func< FileInfo, int, int, ThreadedReader > func = null;
            if ( currOperation.Equals( "decompress" ) )
            {
                func = CreateDecompressReader;
            }
            if ( currOperation.Equals( "compress" ) )
            {
                func = CreateCompressReader;
            }
            Thread thread_Read = null;
            thread_Read = new Thread( () => StartReaders( args [ 1 ], func ) );
            thread_Read.IsBackground = true;

            thread_Read.Start();

            #endregion


            #region Create Writer

            _outFileWriter = new Writer( args [ 2 ], args [ 1 ] );
            _outFileWriter.WorkerSupportsCancellation = true;
            _outFileWriter.RunWorkerAsync();

            #endregion


            //wait for read and write ends
            if ( thread_Read != null)
            {
                while( /*ThreadedReader.registeredReaders.Any(v=>!v.Finished)*/ thread_Read.IsAlive || _outFileWriter.GetQueueCount()>0 )
                {
                    Thread.Sleep( 10 );
                }
            }
            if( _outFileWriter .IsBusy) _outFileWriter.CancelAsync();

            return 0;
        }




        static void StartReaders( object threadData, Func<FileInfo, int, int, ThreadedReader> func)
        {
            string pathToInputFile = ( string ) threadData;
            FileInfo inputFileInfo = new FileInfo( pathToInputFile );

            List<ThreadedReader> gZipThreads = new List< ThreadedReader >();


            #region Divide input file to blocks

            using ( var outFileStream = new FileStream( pathToInputFile, FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                var writeBlockTotalCount = ( int )( outFileStream.Length / BufferSize ) + 1;
                dataBlocks = new DataBlock [ writeBlockTotalCount ];
                for ( int i = 0; i < dataBlocks.Length; i++ )
                {
                    dataBlocks [ i ] = new DataBlock();
                }
            }

            #endregion
            

            #region Read input file by blocks with threads

            for ( int i = 0; i < threadCount; i++ )
            {
                if( func !=null) gZipThreads.Add( func.Invoke( inputFileInfo, threadCount, i ) );
            }

            #endregion


            //wait for threads
            while ( gZipThreads.Any(v=>!v.Finished) )
            {
                Thread.Sleep( 100 );
            }

            gZipThreads.Clear();
            GC.Collect();


/*            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );*/
        }




        static DecompressReader CreateDecompressReader(FileInfo inputFileInfo, int threadCount, int i )
        {
            return new DecompressReader( inputFileInfo, threadCount, i );
        }
        static CompressReader CreateCompressReader ( FileInfo inputFileInfo, int threadCount, int i )
        {
            return new CompressReader( inputFileInfo, threadCount, i );
        }


        #region Utils

        static void ShowInfo ()
        {
            Console.WriteLine( "To zip or unzip files please proceed with the following pattern to type in:\n" +
                               "Zipping: GZipTest.exe compress [Source file path] [Destination file path]\n" +
                               "Unzipping: GZipTest.exe decompress [Compressed file path] [Destination file path]\n" +
                               "To complete the program correct please use the combination CTRL + C" );
        }


        static void CancelKeyPress ( object sender, ConsoleCancelEventArgs _args )
        {
            if ( _args.SpecialKey == ConsoleSpecialKey.ControlC )
            {
                Console.WriteLine( "\nCancelling..." );
                _args.Cancel = true;
                Environment.Exit( 1 );
            }
        }


        public static void ClearCurrentConsoleLine ()
        {
            int currentLineCursor = Console.CursorTop;
            Console.SetCursorPosition( 0, Console.CursorTop );
            Console.Write( new string( ' ', Console.WindowWidth ) );
            Console.SetCursorPosition( 0, currentLineCursor );
        }

        #endregion

    }
}