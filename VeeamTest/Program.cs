﻿using System;
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

        public static int threadCount = Environment.ProcessorCount*5; //magic number 5 !!! because 20 threads work faster than 4
        public static long BufferSize = 1024*1024*1;


        public static DataBlock[] dataBlocks = new DataBlock[0];
        public static ulong dataBlocksBufferedMemoryAmount = 0;
        public static ulong maxMemoryForDataBlocksBuffer = 1024 * 1024 * 100; //500 mb

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
            startTime = DateTime.Now;
            
            Console.CancelKeyPress += new ConsoleCancelEventHandler( CancelKeyPress );
            ShowInfo();

            try
            {
                Validation.StringReadValidation( args );
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
                //ReadCompressed( args[ 1 ] );
                string[] strings = args;
                thread_Read = new Thread(()=> ReadCompressedFile(strings [ 1 ]));
                thread_Read.IsBackground = true;
                thread_Read.Start( );


                //Decompress( args [ 1 ], args [ 2 ] );

                string outputFileName = args[ 1 ].Replace( ".gz", "" );
                if ( args.Length > 2 )
                {
                    outputFileName = args [ 2 ];
                }
                thread_Write = new Thread( ()=> WriteDataBlocksToOutputFile(outputFileName, strings[1]) );
                thread_Write.IsBackground = true;
                thread_Write.Start( );
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                //Compress( args [ 1 ], args [ 2 ] );
                string[] strings = args;

                thread_Read = new Thread( ()=>ReadNotCompressedFile(strings[1]) );
                thread_Read.IsBackground = true;
                thread_Read.Start();


                string outputFileName = args[ 1 ]+".gz";
                if ( args.Length > 2 )
                {
                    outputFileName = args [ 2 ];
                }
                thread_Write = new Thread( ()=> WriteDataBlocksToOutputFile(outputFileName, strings[1]) );
                thread_Write.IsBackground = true;
                thread_Write.Start( );


                //WriteCompressedDataTheaded( args [ 2 ] );
            }


            if( thread_Read != null && thread_Write !=null)
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


            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );
        }

        static void ReadCompressedFile ( string fileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );
            if ( fileToDecompress.Extension != ".gz" ) return;


            #region Read Headers from compressed input file

            List<ThreadedReader> gZipThreads_Headers = new List< ThreadedReader >();


            // Create DataBlocks
            dataBlocks = new DataBlock [ fileToDecompress.Length/BufferSize+1 ];
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

            //order headers
            headersFound = headersFound.Distinct().OrderBy( v => v ).ToList();

/*            using ( FileStream origFileStream = new FileStream( fileToDecompress.FullName, FileMode.Open ) )
            {
                var buffer = new byte[BufferSize*1/*fileToDecompress.Length#1# ];
                byte[] bufferGZipHeader = new byte[4];
                long currPos = 0;
                long diff = 0;


                while ( true )
                {
                    if ( headersFound.Count > 0 )
                    {
                        currPos = headersFound [ headersFound.Count - 1 ] + 3;
                        origFileStream.Position = currPos;
                    }

                    origFileStream.Read( buffer, 0, buffer.Length );

                    //Read headers
                    for ( long i = 0; i < buffer.Length; i++ )
                    {
                        #region Read Header

                        for ( int j = 0; j < bufferGZipHeader.Length; j++ )
                        {
                            if ( buffer.Length > i + j ) //check for i+j < bufferAllFile.Lenght
                            {
                                bufferGZipHeader [ j ] = buffer [ i + j ];
                            }
                        }

                        #endregion


                        //Check header and if true => decompress block
                        if ( bufferGZipHeader [ 0 ] == 0x1F
                                && bufferGZipHeader [ 1 ] == 0x8B
                                && bufferGZipHeader [ 2 ] == 0x08
                                && bufferGZipHeader [ 3 ] == 0x00
                                )
                        {
                            var newHeader = currPos + i;

                            #region Checking
                            
                            if ( headersFound.Count > 0 )
                            {
                                diff = newHeader - headersFound [ headersFound.Count - 1 ];
                            }
                            if ( diff < 0 || newHeader >= origFileStream.Length )
                            {
                                //Console.Write( "\n\n NEGATIVE \n\n" );
                                //headersFound.Add( newHeader );
                                break;
                            }
                            if( headersFound.Contains( newHeader ) ) continue;

                            #endregion

                            headersFound.Add( newHeader );
                            //Console.Write( " H "+ headersFound.Count + " va = " + diff + "  " );
                        }
                    }

                    if ( origFileStream.Position >= origFileStream.Length ) break;
                }
            }*/

            Console.WriteLine( "\nHeaders found " + headersFound.Count );
            Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );

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

            #endregion


            //wait for threads
            while ( gZipThreads.Any( v => !v.Finished ) )
            {
                Thread.Sleep( 100 );
            }

            gZipThreads.Clear();
            GC.Collect();
            

            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );
        }



        static void WriteDataBlocksToOutputFile( string newFileName, string inputFileName )
        {
            //Console.WriteLine( inputFileName );


            FileInfo outFileInfo = new FileInfo( newFileName );
            FileInfo inputFileInfo = new FileInfo( inputFileName );

            //wait for dataBlocks creation
            while ( !readyToWrite/*dataBlocks==null || dataBlocks.Length<1*/ )
            {
                Thread.Sleep( 10 );
            }


            using ( FileStream outFileStream = File.Create( outFileInfo.FullName ) )
            {
                outFileStream.Lock( 0, dataBlocks.Length*BufferSize );


                //for every block do write
                for ( ulong i = 0; i < (ulong)dataBlocks.Length; i++ )
                {
                    DataBlock dataChunk = dataBlocks[ i ];

                    /* wait for chunkData */
            while ( dataChunk.byteData == null )
                    {
                        Thread.Sleep( 10 );
                    }

                    

                    outFileStream.Write( dataChunk.byteData, 0, dataChunk.byteData.Length );

                    dataBlocksBufferedMemoryAmount -= ( ulong ) dataChunk.byteData.Length;
                    dataChunk.byteData=new byte[0];
                    lastWritedDataBlockIndex = i;

                    //Console.Write( " -W " + i );
                    //Console.Clear();
/*                  Console.WriteLine( inputFileName );
                    Console.WriteLine( "GZip Headers found = " + headersFound.Count );*/
                    Console.SetCursorPosition( 0, Console.CursorTop - 1 );
                    ClearCurrentConsoleLine();
                    Console.WriteLine( (int)( (i/( float )dataBlocks.Length)*100 )+ " %. " + " -Write_index " + i );
                }
            }
            Console.WriteLine(  "100 %. " );

            GC.Collect();


            Console.WriteLine( "\nCompressed {0} from {1} to {2} bytes. \nComprRate = {3} X",
                inputFileInfo.Name, inputFileInfo.Length.ToString(), outFileInfo.Length.ToString()
                , ( ( float )inputFileInfo.Length / ( float )outFileInfo.Length ) );
            Console.WriteLine( "" );
            Console.WriteLine( " Write END" );

            Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );
            //Console.ReadKey();

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