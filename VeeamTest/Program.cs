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
                ChooseOperation(  args );
            }

            catch ( Exception ex )
            {
                Console.WriteLine( "Error is occured!\n Method: {0}\n Error description: {1}", ex.TargetSite, ex.Message );
                //return 1;
            }


            Console.WriteLine( string.Format( "\nCompleted in {0} seconds", ( DateTime.Now - startTime ).TotalSeconds ) );

            Console.WriteLine("\nPress any key to exit!" );
            Console.ReadKey();
        }

        
        static int ChooseOperation( string[] args )
        {
            Thread thread_Read = null;
            Thread thread_Write = null;

            currOperation = args[ 0 ].ToLower();
            Console.WriteLine( "\n\n"+currOperation + "ion of " + args [ 1 ] + " started..." );


            if ( args [ 0 ].Equals( "decompress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                thread_Read = new Thread(()=> ReadCompressedFile_New( args [ 1 ]));
                thread_Read.IsBackground = true;
                thread_Read.Start( );
                
/*                thread_Write = new Thread( ()=> WriteDataBlocksToOutputFile(
                    args [ 2 ]/*.Replace( ".gz", "" )#1#,
                    args [ 1 ]) 
                    );
                thread_Write.IsBackground = true;
                thread_Write.Start( );*/
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                thread_Read = new Thread( ()=>ReadNotCompressedFile( args [ 1]) );
                thread_Read.IsBackground = true;
                thread_Read.Start();
                
/*                thread_Write = new Thread( ()=> WriteDataBlocksToOutputFile( args [ 2 ], args [ 1 ]) );
                thread_Write.IsBackground = true;
                thread_Write.Start( );*/
            }


            _outFileWriter = new Writer( args [ 2 ], args [ 1 ] );
            _outFileWriter.RunWorkerAsync();
            _outFileWriter.WorkerSupportsCancellation = true;



            if ( thread_Read != null /*&& thread_Write !=null*/)
            {
                while( thread_Read.IsAlive /*|| thread_Write.IsAlive*/  || _outFileWriter.GetQueueCount()>0 )
                {
                    Thread.Sleep( 10 );
                }
            }
            if( _outFileWriter .IsBusy) _outFileWriter.CancelAsync();

            return 0;
        }




        static void ReadNotCompressedFile( object threadData )
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
                gZipThreads.Add( new CompressReader( inputFileInfo, threadCount, i ) );
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


        static void ReadCompressedFile_New ( string pathToInputFile )
        {
            //string pathToInputFile = ( string ) threadData;
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
                ThreadedReader gZipThread = new DecompressReader(inputFileInfo, threadCount, i);
                gZipThreads.Add( gZipThread );
            }

            #endregion


            //wait for threads
            while ( gZipThreads.Any( v => !v.Finished ) )
            {
                Thread.Sleep( 100 );
            }

            Console.WriteLine( "\nHeaders found = " + DecompressReader.headersFound.Count );


            gZipThreads.Clear();
            GC.Collect();


            /*Console.WriteLine( "" );
            Console.WriteLine( " Read END" );*/
        }



/*
        static void ReadCompressedFile ( string fileName )
        {
            try
            {
                FileInfo fileToDecompress = new FileInfo( fileName );
                //if ( fileToDecompress.Extension != ".gz" ) return;


                #region Read Headers from compressed input file

                Console.WriteLine( "Reading GZip Headers, this can take a few minutes for a large file" );

                List<ThreadedReader> gZipThreads_Headers = new List< ThreadedReader >();


                // Create DataBlocks
                dataBlocks = new DataBlock [ fileToDecompress.Length / BufferSize + 1 ];
                for ( int i = 0; i < dataBlocks.Length; i++ )
                {
                    dataBlocks [ i ] = new DataBlock();
                }

                //threads read headers
                for ( int i = 0; i < threadCount; i++ )
                {
                    ThreadedReader gZipThread = new ThreadedReader(fileToDecompress, threadCount, i);
                    gZipThreads_Headers.Add( gZipThread );
                }

                //wait for threads
                while ( gZipThreads_Headers.Any( v => !v.Finished ) )
                {
                    Thread.Sleep( 100 );
                }


                //check for broken gzip
                if ( headersFound.Count == 0 )
                {
                    throw new Exception( "Source file doesn't contains any compressed data" );
                }


                //order headers
                headersFound = headersFound.Distinct().OrderBy( v => v ).ToList();


                Console.WriteLine( "\nHeaders found " + headersFound.Count );
                Console.WriteLine( string.Format( "Completed in {0} seconds", ( DateTime.Now - startTime ).TotalSeconds ) + "\n\n" );


                #endregion


                #region Create DataBlocks array from  GZipHeaders. Each DB have indexes(start/end) for reading from input file

                dataBlocks = new DataBlock [ headersFound.Count ];
                for ( int i = 0; i < dataBlocks.Length; i++ )
                {
                    long startIndex, endIndex = 0;
                    startIndex = headersFound [ i ];
                    endIndex = i + 1 < headersFound.Count ? headersFound [ i + 1 ] : fileToDecompress.Length + 1;
                    //endIndex = headersFound [ i + 1 ];

                    dataBlocks [ i ] = new DataBlock()
                    {
                        startIndex = startIndex,
                        endIndex = endIndex
                    };
                }

                #endregion

                
                #region Read compressed input file by DataBlocks indexes

                List<ThreadedReader> gZipThreads = new List< ThreadedReader >();

                for ( int i = 0; i < threadCount; i++ )
                {
                    ThreadedReader gZipThread = new ThreadedReader(fileToDecompress, threadCount, i);
                    gZipThreads.Add( gZipThread );
                }


                //wait for threads
                while ( gZipThreads.Any( v => !v.Finished ) )
                {
                    Thread.Sleep( 10 );
                }

                gZipThreads.Clear();
                GC.Collect();

                #endregion

            }

            catch ( Exception ex )
            {
                Console.WriteLine( "Error is occured!\n Method: {0}\n Error description {1}", ex.TargetSite, ex.Message );
            }
        }
*/


       
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


        static void ClearCurrentConsoleLine ()
        {
            int currentLineCursor = Console.CursorTop;
            Console.SetCursorPosition( 0, Console.CursorTop );
            Console.Write( new string( ' ', Console.WindowWidth ) );
            Console.SetCursorPosition( 0, currentLineCursor );
        }

        #endregion

    }
}