import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object gene {
    def main(args: Array[String]){
        val conf = new SparkConf()
        conf.setAppName("gene")
        val sc = new SparkContext(conf)
        if(args.size==0){
            println("args error!")
            return
        }
        if(args(0)=="download")
        {
            val list=args(1)
            val rdd0=sc.textFile(list,64)
            val rdd1=rdd0.map(
                e=>{
                    import htsjdk.samtools._
                    import java.net.{URL,HttpURLConnection}
                    import org.apache.hadoop.conf.Configuration
                    import org.apache.hadoop.fs.FileSystem
                    import org.apache.hadoop.fs.Path
                    import java.io.{File,InputStream,OutputStream,FileOutputStream}
                    val prefix="http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"
                    val path="gene/"
                    val url=prefix+e
                    def size(url:String):Int={
                        val conn= new URL(url).openConnection match{case a:HttpURLConnection=>a}
                        conn.setRequestMethod("HEAD")
                        conn.getInputStream
                        val l=conn.getContentLength
                        conn.disconnect
                        l
                    }
                    val conf=new Configuration
                    conf.set("fs.default.name", "hdfs://gene:9000")
                    val fs=FileSystem.get(conf)
                    def download(url:String,file:String):Int={
                        def transfer(in:InputStream,out:OutputStream):Int ={
                            def transfer_(in:InputStream,out:OutputStream,s:Int):Int ={
                                val buf=new Array[Byte](4096)
                                val len=in.read(buf)
                                if(len != -1){
                                    out.write(buf,0,len)
                                    transfer_(in,out,s+len)
                                }
                                else{
                                    s
                                }
                            }
                            transfer_(in,out,0)
                        }
                        val u=new URL(url)
                        val conn=u.openConnection
                        val is=conn.getInputStream
                        val os=fs.create(new Path(path+file),true)
                        val sz=transfer(is,os)
                        is.close
                        os.flush
                        os.close
                        sz
                    }
                    try{
                        val l=size(url)
                        val sz=download(url,e)
                        (e,l==sz,sz,l)
                    }catch{
                        case ex:Exception=>
                        (e,false,0,0)
                    }
                }
            )
            rdd1.saveAsTextFile(args(2))
        }
        if(args(0)=="check")
        {
            val rdd0=sc.textFile(args(1))
            val rdd1=rdd0.map(
                e=>{
                    val r="\\((.+),(.+),(.+),(.+)\\)".r
                    e match{
                    case r(f,b,s,l) => 
                        (f,b.toBoolean,s.toInt,l.toInt)
                    case _ => ("",true,0,0)
                    }
                }
            )
            val rdd2=rdd1.filter(
                e=> !e._2
            )
            val rdd3=rdd2.map(
                e=> e._1
            )
            rdd3.persist
            rdd3.foreach(
                e=>{
                    import org.apache.hadoop.conf.Configuration
                    import org.apache.hadoop.fs.FileSystem
                    import org.apache.hadoop.fs.Path
                    val path="gene/"
                    val conf=new Configuration
                    conf.set("fs.default.name", "hdfs://gene:9000")
                    val fs=FileSystem.get(conf)
                    fs.delete(new Path(path+e),false)
                }
            )
            rdd3.saveAsTextFile(args(2))
            println(rdd3.collect.size)
        }
        if(args(0)=="process")
        {
            val rdd0=sc.binaryFiles(args(1),256)
            val rdd1=rdd0.filter(
                e=>{
                    e._1.endsWith(".bam")
                }
            )
            val rdd2=rdd1.flatMap(
                e=>{
                    import htsjdk.samtools._
                    import java.net.URL
                    import java.io.{File,InputStream,OutputStream,FileOutputStream}
                    def readAllUnmapped(iter:SAMRecordIterator):Seq[Tuple3[Int,Int,String]]={
                        def sub(a:Seq[Tuple3[Int,Int,String]]):Seq[Tuple3[Int,Int,String]]={
                            if(iter.hasNext){
                                val e=iter.next
                                if(e.getReadUnmappedFlag){
                                    sub(a:+(e.getAlignmentStart,e.getReadLength,e.getReadString))
                                }
                                else{
                                    sub(a)
                                }
                            }
                            else{
                                a
                            }
                        }
                        sub(Seq())
                    }
                    try{
                        val file=e._1
                        val is=e._2.open
                        val input=SamInputResource.of(is)
                        val reader=SamReaderFactory.makeDefault().open(input)
                        val r=readAllUnmapped(reader.iterator)
                        is.close
                        r
                    }catch{
                        case ex:Exception=>
                            println("exception: "+ex)
                            println("file: "+e._1)
                            Seq()
                        case u:Throwable => println(u)
                        Seq()                   
                    }
                }
            )
            rdd2.saveAsTextFile(args(2))
        }
        if(args(0)=="kmers")
        {
            val rdd0=sc.textFile(args(1))
            val rdd1=rdd0.flatMap(
                e=>{
                    try{
                        val r="\\((.+),(.+),(.+)\\)".r
                        e match{
                        case r(s,l,b) => 
                            Seq((s.toInt,l.toInt,b.toString))
                        case _ => Seq()
                        }
                    }catch{
                    case ex:Throwable=>
                        println("ex: "+ex)
                        println("line: "+e)
                        Seq()
                    }
                }
            )
            val rdd2=rdd1.flatMap(
                e=>{
                    def kmers(s:String,k:Int):List[String]={
                        if(s.length<k){
                            List()
                        }
                        else{
                            s.substring(0,k)::kmers(s.substring(1),k)
                        }
                    }
                    kmers(e._3,10)
                }
            )
            val rdd3=rdd2.map(
                e=>(e,1)
            )
            val rdd4=rdd3.reduceByKey(
                (e1,e2)=>(e1+e2)
            )
            val rdd5=rdd4.filter(
                e=>{
                    e._2>=10&&e._2<=200
                }
            )
            rdd5.saveAsTextFile(args(2))
        }
        if(args(0)=="insert")
        {
            val rdd0=sc.textFile(args(1))
            val rdd1=rdd0.flatMap(
                e=>{
                    try{
                        val r="\\((.+),(.+),(.+)\\)".r
                        e match{
                        case r(s,l,b) =>
                            Seq((s.toInt,l.toInt,b.toString))
                        case _ => Seq()
                        }
                    }catch{
                    case ex:Throwable=>
                        println("ex: "+ex)
                        println("line: "+e)
                        Seq()
                    }
                }
            )
            val rdd2=rdd1.map(
                e=>e._1
            )
            rdd2.saveAsTextFile(args(2))
        }
    }
}

