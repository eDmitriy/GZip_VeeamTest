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

        public static int threadCount = Environment.ProcessorCount*1; //magic number 5 !!! because 20 threads work faster than 4
        public static long BufferSize = 1024*1024;


        public static DataBlock[] dataBlocks = new DataBlock[0];
        public static ulong dataBlocksBufferedMemoryAmount = 0;
        public static ulong maxMemoryForDataBlocksBuffer = 1024 * 1024 * 500; //500 mb



        static DateTime startTime;

        #endregion




        static void Main ( string [] args )
        {
            int nBufferWidth = Console.BufferWidth;
            int nBufferHeight = 5001;
            Console.SetBufferSize( nBufferWidth, nBufferHeight );


            startTime = DateTime.Now;
            if( ChooseAction( ref args ) == 0 )
            {
                Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );
                Console.ReadKey();
            }

        }






        static int ChooseAction( ref string[] args )
        {
            if( args.Length < 2 )
            {
                Console.WriteLine("Wrong parameters, see help/?");
                Console.ReadKey();
                return 1;
            }

            if ( args [ 0 ].Equals( "decompress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                //ReadCompressed( args[ 1 ] );
                string[] strings = args;
                Thread thread = new Thread(()=> ReadCompressedFile(strings [ 1 ]));
                thread.IsBackground = true;
                thread.Start( );


                //Decompress( args [ 1 ], args [ 2 ] );
                Thread thread2 = new Thread( WriteCompressedDataTheaded );
                thread2.IsBackground = true;
                string outputFileName = args[ 1 ].Replace( ".gz", "" );
                if ( args.Length > 2 )
                {
                    outputFileName = args [ 2 ];
                }
                thread2.Start( outputFileName );
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                //Compress( args [ 1 ], args [ 2 ] );

                Thread thread = new Thread( ReadNotCompressedFile );
                thread.IsBackground = true;

                Thread thread2 = new Thread( WriteCompressedDataTheaded );
                thread2.IsBackground = true;

                thread.Start(args[1]);


                string outputFileName = args[ 1 ]+".gz";
                if ( args.Length > 2 )
                {
                    outputFileName = args [ 2 ];
                }
                thread2.Start( outputFileName );


                //WriteCompressedDataTheaded( args [ 2 ] );
            }

            return 0;
        }




        static void ReadNotCompressedFile( object threadData )
        {
            string pathToInputFile = ( string ) threadData;
            FileInfo inputFileInfo = new FileInfo( pathToInputFile );

            #region InitChunks

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

            #region InitThreads-Read

            List<ThreadedReader> gZipThreads = new List< ThreadedReader >();

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

            while( gZipThreads.Any(v=>!v.Finished) )
            {
                //wait for threads
            }
            gZipThreads.Clear();
            GC.Collect();


            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );
        }

        static void WriteCompressedDataTheaded( object threadData )
        {
            string newFileName = ( string ) threadData;
            FileInfo outFileInfo = new FileInfo( newFileName );

            while ( dataBlocks==null || dataBlocks.Length<1 )
            {
                //wait for chunks creation
            }


            using ( FileStream outFileStream = File.Create( outFileInfo.FullName ) )
            {
                outFileStream.Lock( 0, dataBlocks.Length*BufferSize );

                long index = 0;

                //for every block do write
                for ( int i = 0; i < dataBlocks.Length; i++ )
                {
                    index = i;

                    while( dataBlocks [i].byteData==null ){/* wait for chunkData */}
                    
                    DataBlock dataChunk = dataBlocks[ index ];
                    outFileStream.Write( dataChunk.byteData, 0, dataChunk.byteData.Length );

                    dataBlocksBufferedMemoryAmount -= ( ulong ) dataChunk.byteData.Length;
                    dataChunk.byteData=new byte[0];

                    Console.Write( " -W " + index );
                }
            }


/*                FileInfo info = new FileInfo(fileInfo.Directory + "\\" + newFileName/*fileInfo.Name + ".gz"#1#);
            Console.WriteLine( "\nCompressed {0} from {1} to {2} bytes. \nComprRate = {3} X",
                fileInfo.Name, fileInfo.Length.ToString(), info.Length.ToString()
                , ( ( float )fileInfo.Length / ( float )info.Length ) );*/
            Console.WriteLine( "" );
            Console.WriteLine( " Write END" );

            Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );
            Console.ReadKey();

        }





        static void ReadCompressedFile( string fileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );
            if ( fileToDecompress.Extension != ".gz" ) return;

            List<long> headersFound = new List< long >();


            #region ReadHeders

            using ( FileStream origFileStream = new FileStream( fileToDecompress.FullName, FileMode.Open ) )
            {
                var buffer = new byte[BufferSize*1/*fileToDecompress.Length*/ ];
                byte[] bufferGZipHeader = new byte[4];
                long currPos = 0;
                long diff = 0;


                while ( true )
                {
                    if( headersFound.Count>0 )
                    {
                        currPos = headersFound[ headersFound.Count - 1 ] + 3;
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
                        if (    bufferGZipHeader [ 0 ] == 0x1F
                                && bufferGZipHeader [ 1 ] == 0x8B
                                && bufferGZipHeader [ 2 ] == 0x08
                                && bufferGZipHeader [ 3 ] == 0x00
                                )
                        {
                            var newHeader = currPos + i;

                            if ( headersFound.Count > 0 )
                            {
                                diff = newHeader - headersFound[ headersFound.Count - 1 ] ;
                            }
                            if( diff < 0 
                                || headersFound .Contains( newHeader ) 
                                || newHeader >= origFileStream.Length )
                            {
                                //Console.Write( "\n\n NEGATIVE \n\n" );
                                //headersFound.Add( newHeader );
                                break;
                            }
                            headersFound.Add( newHeader );
                            //Console.Write( " H "+ headersFound.Count + " va = " + diff + "  " );
                        }
                    }

                    if ( origFileStream.Position >= origFileStream.Length )break;
                }
            }

            Console.WriteLine("\nHeaders found " + headersFound.Count);
            Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - startTime ).TotalSeconds ) );
            //Console.ReadKey();



            #endregion


            #region DivideToDataBlocks

            dataBlocks = new DataBlock [ headersFound.Count];
            for ( int i = 0; i < dataBlocks.Length; i++ )
            {
                long startIndex, endIndex = 0;
                startIndex = headersFound[ i ];
                endIndex = i+1 < headersFound.Count ? headersFound [ i + 1 ] : fileToDecompress.Length+1;
                //endIndex = headersFound [ i + 1 ];

                dataBlocks [ i ] = new DataBlock()
                {
                    startIndex = startIndex,
                    endIndex = endIndex
                };
            }

            #endregion


            #region Init Threaded Reading & Compression
            
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



            while ( gZipThreads.Any( v => !v.Finished ) )
            {
                //wait for threads
            }
            gZipThreads.Clear();
            GC.Collect();



            Console.WriteLine( "" );
            Console.WriteLine( " Read END" );
        }


/*
        static void Decompress ( string fileName, string newFileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );
            if( fileToDecompress.Extension != ".gz" ) return;

            //long originalFileSize = fileToDecompress.Length;
            while( readCompressed == null || readCompressed.Length == 0 )
            {
                //Console.WriteLine("Wait for read threadWorkers starts");
            }

            using ( FileStream decompressedFileStream = File.Create( newFileName ) )
            {
                int threadsIndex = 0;
                int writeCounter = 1;


                //decompress
                long length = readCompressed.Length-1;

                while ( readCompressed [ length ] == null 
                    || !readCompressed [ length ].Finished 
                    || readCompressed [ length ].dataBlocksQueue.Count > 0 
                    /*readCompressed.Any(v=>v==null || ( v.Finished==false || v.dataBlocksQueue.Count>0)) #1#)
                {
                    //long newPos = 0;
                    var thread = readCompressed[ threadsIndex ];
                    if(thread==null) continue;
                    if( thread.Finished && thread.dataBlocksQueue.Count == 0)
                    {
                        threadsIndex++;
                        continue;
                    }
                    if( thread.dataBlocksQueue.Count == 0) continue;


                    var newDataBlock = thread.dataBlocksQueue.Dequeue();
                    decompressedFileStream.Write( newDataBlock.byteData, 0, newDataBlock.byteData.Length );

                    Console.WriteLine( /*"decStr pos = " + originalFileStream.Position
                                        + #1#" counter = " + writeCounter++ );
                }
            }
            Console.WriteLine( "Decompressed: {0}", fileToDecompress.Name );
            GC.Collect();

        }

*/

    }
}


public class DataBlock
{
    public long startIndex;
    public long endIndex;

    public byte[] byteData;
}