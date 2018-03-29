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
        public static ulong dataBlocksBufferedMemoryAmount = 0;
        public static ulong maxMemoryForDataBlocksBuffer = 1024 * 1024 * 100; //100 mb

        public static List<long> headersFound = new List< long >();
        public static bool readyToWrite;
        public static ulong lastWritedDataBlockIndex = 0;


        static DateTime startTime;
        private static string currOperation = "";


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
                thread_Read = new Thread(()=> ReadCompressedFile( args [ 1 ]));
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


            readyToWrite = true;


            #region Read input file by blocks with threads

            for ( int i = 0; i < threadCount; i++ )
            {
                //CompressionThread gZipThread = new CompressionThread(inputFileInfo, threadCount, i);
                ThreadedReader gZipThread = new ThreadedReader(
                    inputFileInfo, threadCount, i, ThreadedReader.WorkType.readNotCompressed
                    //ThreadedReader.ReadBytesBlockForCompression
                    );

                gZipThreads.Add( gZipThread );
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
                    ThreadedReader gZipThread = new ThreadedReader(
                    fileToDecompress, threadCount, i, ThreadedReader.WorkType.readHeaders
                    //ThreadedReader.ReadHeaders
                );
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


                readyToWrite = true;


                #region Read compressed input file by DataBlocks indexes

                List<ThreadedReader> gZipThreads = new List< ThreadedReader >();

                for ( int i = 0; i < threadCount; i++ )
                {
                    ThreadedReader gZipThread = new ThreadedReader(
                        fileToDecompress, threadCount, i, ThreadedReader.WorkType.readCompressed
                        //ThreadedReader.ReadBytesBlockForDecompression
                    );
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



        static void WriteDataBlocksToOutputFile( string newFileName, string inputFileName )
        {
            FileInfo outFileInfo = new FileInfo( newFileName );
            FileInfo inputFileInfo = new FileInfo( inputFileName );

            //wait for dataBlocks creation
            while ( !readyToWrite )
            {
                Thread.Sleep( 10 );
            }

            //write every dataBlock continuously or wait for dataBlock become ready
            using ( FileStream outFileStream = File.Create( outFileInfo.FullName ) )
            {
                outFileStream.Lock( 0, dataBlocks.Length*BufferSize );


                //for every block do write
                for ( ulong i = 0; i < (ulong)dataBlocks.Length; i++ )
                {
                    DataBlock dataBlock = dataBlocks[ i ];

                    /* wait for dataBlock bytes ready */
                    while ( dataBlock.ByteData == null )
                    {
                        Thread.Sleep( 10 );
                    }
                    

                    outFileStream.Write( dataBlock.ByteData, 0, dataBlock.ByteData.Length );

                    dataBlocksBufferedMemoryAmount -= ( ulong )dataBlock.ByteData.Length;
                    dataBlock.ByteData = new byte[0];
                    lastWritedDataBlockIndex = i;


                    #region Console info

                    //Console.Write( " -W " + i );
                    Console.SetCursorPosition( 0, Console.CursorTop - 1 );
                    ClearCurrentConsoleLine();
                    Console.WriteLine( (int)( (i/( float )dataBlocks.Length)*100 )+ " %. " + " -Write_index " + i );

                    #endregion
                }
            }

            //finished
            GC.Collect();


            #region Write to console final results

            #region ComperssionRate

            float comprRate = 0;
            switch ( currOperation )
            {
                case "compress":
                    comprRate = ( ( float )inputFileInfo.Length / ( float )outFileInfo.Length );
                    break;
                case "decompress":
                    comprRate =  ( float )outFileInfo.Length / ( float )inputFileInfo.Length;
                    break;
                default:
                    break;
            }

            #endregion

            Console.WriteLine( "100 %. " );

            Console.WriteLine("\n"+ currOperation+"ion "+ "{0} from {1} to {2} bytes. \nCompression Rate = {3} X",
                inputFileInfo.Name, inputFileInfo.Length.ToString(), outFileInfo.Length.ToString(), comprRate );
            //Console.WriteLine( "\n Write END" );

            Console.WriteLine( string.Format( "Completed in {0} seconds", ( DateTime.Now - startTime ).TotalSeconds ) );

            #endregion
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