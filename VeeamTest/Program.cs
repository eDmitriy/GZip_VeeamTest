using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using Microsoft.VisualBasic.Devices;


namespace VeeamTest
{
    class Program
    {
        #region Vars

        public static int threadCount = Environment.ProcessorCount;
        public static long BufferSize = 1024*1024*1;


        public static DataBlock[] dataBlocks = new DataBlock[0];
        public static ulong dataBlocksBufferedMemoryAmount = 0;
        public static ulong maxMemoryForDataBlocksBuffer = 1024 * 1024 * 100; //100 mb

        public static List<long> headersFound = new List< long >();
        public static bool readyToWrite;
        public static ulong lastWritedDataBlockIndex = 0;


        static DateTime startTime;

        #endregion

        

        static void Main ( string [] args )
        {
/*            int nBufferWidth = Console.BufferWidth;
            int nBufferHeight = 5001;
            Console.SetBufferSize( nBufferWidth, nBufferHeight );*/
            
            Console.CancelKeyPress += new ConsoleCancelEventHandler( CancelKeyPress );
            ShowInfo();

            try
            {
                #region Validation

                //memory check
                if( new ComputerInfo().AvailableVirtualMemory / ( 1024 * 1024 ) < 500 )
                {
                    throw new Exception( "This program requires at least 500 mb of free RAM" );
                }
                Validation.StringReadValidation( args );

                #endregion

                startTime = DateTime.Now;
                ChooseAction(  args );
            }

            catch ( Exception ex )
            {
                Console.WriteLine( "Error is occured!\n Method: {0}\n Error description {1}", ex.TargetSite, ex.Message );
                //return 1;
            }
            Console.WriteLine("press any key to exit!" );
            Console.ReadKey();
        }

        


        static int ChooseAction( string[] args )
        {
            Thread thread_Read = null;
            Thread thread_Write = null;

            if ( args [ 0 ].Equals( "decompress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                thread_Read = new Thread(()=> ReadCompressedFile( args [ 1 ]));
                thread_Read.IsBackground = true;
                thread_Read.Start( );
                
                thread_Write = new Thread( ()=> WriteDataBlocksToOutputFile(
                    args [ 2 ]/*.Replace( ".gz", "" )*/,
                    args [ 1 ]) 
                    );
                thread_Write.IsBackground = true;
                thread_Write.Start( );

                Console.WriteLine( "\n\nDecompressing of " + args [ 1 ] + " started..." );
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                thread_Read = new Thread( ()=>ReadNotCompressedFile( args [ 1]) );
                thread_Read.IsBackground = true;
                thread_Read.Start();
                
                thread_Write = new Thread( ()=> WriteDataBlocksToOutputFile( args [ 2 ], args [ 1 ]) );
                thread_Write.IsBackground = true;
                thread_Write.Start( );
                
                Console.WriteLine( "\n\nCompression of " + args [ 1 ] + " started..." );
            }


            if ( thread_Read != null && thread_Write !=null)
            {
                while( thread_Read.IsAlive || thread_Write.IsAlive )
                {
                    Thread.Sleep( 10 );
                }
            }
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
                    inputFileInfo, threadCount, i, 
                    ThreadedReader.ReadBytesBlockForCompression
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
                if ( fileToDecompress.Extension != ".gz" ) return;


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
                    fileToDecompress, threadCount, i,
                    ThreadedReader.ReadHeaders
                    //, true
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
                Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) + "\n\n" );


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
                    fileToDecompress, threadCount, i,
                    ThreadedReader.ReadBytesBlockForDecompression
                    );
                    gZipThreads.Add( gZipThread );
                }


                //wait for threads
                while ( gZipThreads.Any( v => !v.Finished ) )
                {
                    Thread.Sleep( 100 );
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
                    while ( dataBlock.byteData == null )
                    {
                        Thread.Sleep( 10 );
                    }
                    

                    outFileStream.Write( dataBlock.byteData, 0, dataBlock.byteData.Length );

                    dataBlocksBufferedMemoryAmount -= ( ulong )dataBlock.byteData.Length;
                    dataBlock.byteData = new byte[0];
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
            Console.WriteLine(  "100 %. " );
            GC.Collect();


            #region Write to console final results

            Console.WriteLine( "\nCompressed {0} from {1} to {2} bytes. \nComprRate = {3} X",
                inputFileInfo.Name, inputFileInfo.Length.ToString(), outFileInfo.Length.ToString()
                , ( ( float )inputFileInfo.Length / ( float )outFileInfo.Length ) );
            Console.WriteLine( "" );
            Console.WriteLine( " Write END" );

            Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );
            Console.WriteLine( outFileInfo.FullName  );

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


public class DataBlock
{
    public long startIndex;
    public long endIndex;

    public byte[] byteData;
}