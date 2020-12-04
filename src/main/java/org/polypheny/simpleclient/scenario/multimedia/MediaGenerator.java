/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.polypheny.simpleclient.scenario.multimedia;


import static javax.sound.sampled.AudioFormat.Encoding.PCM_SIGNED;

import io.humble.video.Codec;
import io.humble.video.Encoder;
import io.humble.video.MediaPacket;
import io.humble.video.MediaPicture;
import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;
import io.humble.video.PixelFormat;
import io.humble.video.Rational;
import io.humble.video.awt.MediaPictureConverter;
import io.humble.video.awt.MediaPictureConverterFactory;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.imageio.ImageIO;
import javax.sound.sampled.AudioFileFormat.Type;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;


public final class MediaGenerator {

    public static BufferedImage generateRandomBufferedImg( int height, int width ) {
        //see https://dyclassroom.com/image-processing-project/how-to-create-a-random-pixel-image-in-java
        BufferedImage img = new BufferedImage( width, height, BufferedImage.TYPE_INT_ARGB );
        for ( int y = 0; y < height; y++ ) {
            for ( int x = 0; x < width; x++ ) {
                int a = ThreadLocalRandom.current().nextInt( 0, 256 );
                int r = ThreadLocalRandom.current().nextInt( 0, 256 );
                int g = ThreadLocalRandom.current().nextInt( 0, 256 );
                int b = ThreadLocalRandom.current().nextInt( 0, 256 );
                int p = (a << 24) | (r << 16) | (g << 8) | b; //pixel
                img.setRGB( x, y, p );
            }
        }
        return img;
    }

    public static byte[] generateRandomImg( int height, int width ) {
        BufferedImage img = generateRandomBufferedImg( height, width );
        //see https://www.tutorialspoint.com/How-to-convert-Image-to-Byte-Array-in-java
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ImageIO.write( img, "png", bos );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }

    public static byte[] generateRandomWav( int sizeKB ) {
        //see https://www.programcreek.com/java-api-examples/?class=javax.sound.sampled.AudioSystem&method=write
        AudioFormat format = new AudioFormat( PCM_SIGNED, 44100, 8, 1, 1, 44100, false );
        AudioInputStream ais = new AudioInputStream( new RandomInputStream( sizeKB * 1000 ), format, sizeKB * 1000 );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            AudioSystem.write( ais, Type.WAVE, baos );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        return baos.toByteArray();
    }

    static class RandomInputStream extends InputStream {

        Random random = new Random();
        int counter;

        RandomInputStream( int size ) {
            this.counter = size;
        }

        @Override
        public int read() throws IOException {
            if ( counter-- == 0 ) {
                return -1;
            }
            return random.nextInt( 256 );
        }
    }

    public static synchronized File generateRandomVideoFile( int numberOfFrames, int width, int height ) {
        //see https://github.com/artclarke/humble-video/blob/master/humble-video-demos/src/main/java/io/humble/video/demos/RecordAndEncodeVideo.java

        File out = new File( System.getProperty( "user.home" ), ".polypheny/tmp/" + UUID.randomUUID().toString() + ".avi" );
        final Muxer muxer = Muxer.make( out.getAbsolutePath(), null, "avi" );
        final MuxerFormat format = muxer.getFormat();
        final Codec codec = Codec.findEncodingCodec( format.getDefaultVideoCodecId() );
        Encoder encoder = Encoder.make( codec );
        encoder.setWidth( width );
        encoder.setHeight( height );
        // We are going to use 420P as the format because that's what most video formats these days use
        final PixelFormat.Type pixelformat = PixelFormat.Type.PIX_FMT_YUV420P;
        encoder.setPixelFormat( pixelformat );
        final Rational framerate = Rational.make( 1, 2 );//den: frames per second
        encoder.setTimeBase( framerate );
        if ( format.getFlag( MuxerFormat.Flag.GLOBAL_HEADER ) ) {
            encoder.setFlag( Encoder.Flag.FLAG_GLOBAL_HEADER, true );
        }
        encoder.open( null, null );
        muxer.addNewStream( encoder );
        try {
            muxer.open( null, null );
        } catch ( InterruptedException | IOException e ) {
            e.printStackTrace();
        }

        MediaPictureConverter converter = null;
        final MediaPicture picture = MediaPicture.make(
                encoder.getWidth(),
                encoder.getHeight(),
                pixelformat );
        picture.setTimeBase( framerate );

        final MediaPacket packet = MediaPacket.make();
        for ( int i = 0; i < numberOfFrames; i++ ) {
            BufferedImage img = convertToType( generateRandomBufferedImg( height, width ), BufferedImage.TYPE_3BYTE_BGR );
            // This is LIKELY not in YUV420P format, so we're going to convert it using some handy utilities.
            if ( converter == null ) {
                converter = MediaPictureConverterFactory.createConverter( img, picture );
            }
            converter.toPicture( picture, img, i );

            do {
                encoder.encode( packet, picture );
                if ( packet.isComplete() ) {
                    muxer.write( packet, false );
                }
            } while ( packet.isComplete() );
        }

        do {
            encoder.encode( packet, null );
            if ( packet.isComplete() ) {
                muxer.write( packet, false );
            }
        } while ( packet.isComplete() );

        muxer.close();
        return out;
    }

    //from https://github.com/artclarke/humble-video/blob/master/humble-video-demos/src/main/java/io/humble/video/demos/RecordAndEncodeVideo.java
    public static BufferedImage convertToType( BufferedImage sourceImage, int targetType ) {
        BufferedImage image;
        // if the source image is already the target type, return the source image
        if ( sourceImage.getType() == targetType ) {
            image = sourceImage;
        }
        // otherwise create a new image of the target type and draw the new image
        else {
            image = new BufferedImage( sourceImage.getWidth(),
                    sourceImage.getHeight(), targetType );
            image.getGraphics().drawImage( sourceImage, 0, 0, null );
        }
        return image;
    }

    public static String insertByteHexString( byte[] bytes ) {
        if ( bytes == null ) {
            return "NULL";
        } else {
            //see https://www.programiz.com/java-programming/examples/convert-byte-array-hexadecimal#:~:text=To%20convert%20byte%20array%20to%20a%20hex%20value%2C%20we%20loop,for%20large%20byte%20array%20conversion.
            StringBuilder builder = new StringBuilder( "x'" );
            for ( byte b : bytes ) {
                builder.append( String.format( "%02X", b ) );
            }
            builder.append( "'" );
            return builder.toString();
        }
    }

    /**
     * Reads all bytes of a file and then deletes it
     */
    public static byte[] getVideo( File videoFile ) {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes( videoFile.toPath() );
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
        videoFile.delete();
        return bytes;
    }

    public static Timestamp randomTimestamp() {
        //see https://stackoverflow.com/questions/11016336/how-to-generate-a-random-timestamp-in-java
        long offset = Timestamp.valueOf( "2000-01-01 00:00:00" ).getTime();
        long end = Timestamp.valueOf( "2020-01-01 00:00:00" ).getTime();
        long diff = end - offset + 1;
        return new Timestamp( offset + (long) (Math.random() * diff) );
    }

}
