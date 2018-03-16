using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;


namespace VeeamTest
{
    class Program
    {
        static void Main ( string [] args )
        {
            var start = DateTime.Now;
            ChooseAction( ref args );

            Console.WriteLine( string.Format( "Completed in {0}", ( DateTime.Now - start ).TotalSeconds ) );
            Console.ReadKey();
        }






        static void ChooseAction( ref string[] args )
        {
            if(args.Length < 2) return;

            if ( args [ 0 ].Equals( "decompress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                Decompress( args [ 1 ] );
            }
            if ( args [ 0 ].Equals( "compress", StringComparison.InvariantCultureIgnoreCase ) )
            {
                Compress( args [ 1 ] );
            }
        }




        static void Compress(string pathToInputFile)
        {
            Console.WriteLine("Compress");

            using ( var fStream = new FileStream( pathToInputFile, FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                var fileInfo = new FileInfo( pathToInputFile );


                if ( ( File.GetAttributes( fileInfo.FullName ) &
                        FileAttributes.Hidden ) != FileAttributes.Hidden & fileInfo.Extension != ".gz" )
                {
                    using ( FileStream compressedFileStream = File.Create( fStream.Name + ".gz" ) )
                    {
                        using ( GZipStream compressionStream = new GZipStream( compressedFileStream,
                            CompressionMode.Compress ) )
                        {
                            fStream.CopyTo( compressionStream );
                        }
                    }
                    FileInfo info = new FileInfo(fileInfo.Directory + "\\" + fileInfo.Name + ".gz");
                    Console.WriteLine( "Compressed {0} from {1} to {2} bytes. \nComprRate = {3} X",
                        fileInfo.Name, fileInfo.Length.ToString(), info.Length.ToString()
                        ,( (float)fileInfo.Length / (float)info.Length) );
                }

            }
        }





        public static void Decompress ( string fileName )
        {
            FileInfo fileToDecompress = new FileInfo( fileName );


            using ( FileStream originalFileStream = fileToDecompress.OpenRead() )
            {
                string currentFileName = fileToDecompress.FullName;
                string newFileName = currentFileName.Remove(currentFileName.Length - fileToDecompress.Extension.Length);

                using ( FileStream decompressedFileStream = File.Create( newFileName ) )
                {
                    using ( GZipStream decompressionStream = new GZipStream( originalFileStream, CompressionMode.Decompress ) )
                    {
                        decompressionStream.CopyTo( decompressedFileStream );
                        Console.WriteLine( "Decompressed: {0}", fileToDecompress.Name );
                    }
                }
            }
        }


    }
}
