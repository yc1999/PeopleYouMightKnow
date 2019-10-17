import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class PeopleYouMightKnow {
    public static class  PairMapper extends Mapper<Object, Text, LongWritable,Text>{

        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{
            //获取一行输入
            String[] inputLine = value.toString().split("\t");

            //获取源节点
            LongWritable source = new LongWritable(Long.valueOf(inputLine[0]));
            //目标节点列表
            List<String> targets = new ArrayList<String>();

            //因为有些输入没有target，坑。
            if (inputLine.length == 2){
                //原文件是以逗号为分隔符的
                StringTokenizer itr = new StringTokenizer(inputLine[1],",");
                while(itr.hasMoreTokens()){
                    String target = itr.nextToken();
                    targets.add(target);
                    //我们要记录一下source已有的朋友，坑。其中，我们使用1来表示已经是直接的朋友了，0表示是间接朋友
                    context.write(source,new Text("1"+target));
                }

                for (int i = 0;i < targets.size();i++){
                    for(int j = i + 1;j < targets.size();j++){
                        context.write(new LongWritable(Long.valueOf(targets.get(i))),new Text("0"+targets.get(j)));
                        context.write(new LongWritable(Long.valueOf(targets.get(j))),new Text("0"+targets.get(i)));
                    }
                }
            }
        }
    }

    public static class CountReducer extends Reducer<LongWritable,Text,LongWritable,Text>{

        public void reduce(LongWritable key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{
            //Map：Long key表示target，Long value表示间接关联度大小，其中为-1表示两者为直接朋友
            final Map<Long, Integer> mutualFriends = new HashMap<Long, Integer>();

            //读取数据，获取Map对。
            for (Text val:values){
                String value = val.toString();
                final boolean check = value.startsWith("1");
                final Long target = Long.valueOf(value.substring(1));

                if(check){
                    //如果当前读入表示是直接朋友，那么设置值为-1
                    mutualFriends.put(target,-1);
                }
                else{
                    //如果当前读入表示不是直接朋友，判断是否在Map中
                    if (mutualFriends.containsKey(target)){
                        //判断之前读入是否是直接朋友，不是则加一
                        if (mutualFriends.get(target) != -1){
                            mutualFriends.put(target,mutualFriends.get(target)+1);
                        }
                    }else{
                        //不在则新建
                        mutualFriends.put(target,1);
                    }
                }
            }

            //设置一个排序的Map
            SortedMap<Long,Integer> sortedMutualFriends = new TreeMap<Long, Integer>(new Comparator<Long>() {
                public int compare(Long o1, Long o2) {
                    Integer v1 = mutualFriends.get(o1);
                    Integer v2 = mutualFriends.get(o2);
                    if(v1 > v2){
                        return -1;
                    }else if(v1.equals(v2) && o1 < o2){
                        return -1;
                    }else{
                        return 1;
                    }
                }
            });

            //将所有的间接朋友放入sortedMutualFriends中
            for (Long target : mutualFriends.keySet()){
                if(mutualFriends.get(target) != -1){
                    sortedMutualFriends.put(target,mutualFriends.get(target));
                }
            }

            int i = 0;
            String output = "";

            //写入输出结果
            for (Long target : sortedMutualFriends.keySet()){
                if (i == 0){
                    output += target;
                }else{
                    output += ","+target;
                }
                i++;
                if (i >= 10){
                    break;
                }
            }

            context.write(key,new Text(output));
        }
    }

    public static void main(String[] args) throws Exception{
        org.apache.hadoop.conf.Configuration conf = new Configuration();

        Job job = new Job(conf,"PeopleYouMightKnow");
        job.setJarByClass(PeopleYouMightKnow.class);

        job.setMapperClass(PairMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //忘记FileInputFormat，FileOutputFormat是什么了
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
