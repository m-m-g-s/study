package mmgs.study.bigdata.hadoop.mr.sifinder;

import mmgs.study.bigdata.hadoop.mr.sifinder.PinyouidPartitioner;
import mmgs.study.bigdata.hadoop.mr.sifinder.PinyouidTimestampWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class PinyouidPartitionerTest {
    private PinyouidPartitioner partitioner = new PinyouidPartitioner();
    private Text nothing = new Text("1");
    private int partitionsAmt = 2;

    @Test
    public void getPartitionFirstDifferent() throws Exception {
        PinyouidTimestampWritable record1 = new PinyouidTimestampWritable("Vh1_O55PDtSUDQb", "20130606000829000");
        PinyouidTimestampWritable record2 = new PinyouidTimestampWritable("ZYqzZA5EDlzWqt", "20130606001605800");
        int partition1 = partitioner.getPartition(record1, nothing, partitionsAmt);
        int partition2 = partitioner.getPartition(record2, nothing, partitionsAmt);
        assertThat("Different records get to different partitions", partition1, not(equalTo(partition2)));
    }

    @Test
    public void getPartitionSecondDifferent() throws Exception {
        PinyouidTimestampWritable record1 = new PinyouidTimestampWritable("Vh1_O55PDtSUDQb", "20130606000829000");
        PinyouidTimestampWritable record2 = new PinyouidTimestampWritable("Vh1_O55PDtSUDQb", "20130606001605800");
        int partition1 = partitioner.getPartition(record1, nothing, partitionsAmt);
        int partition2 = partitioner.getPartition(record2, nothing, partitionsAmt);
        assertThat("Records with equal pinyouid get to the same partition", partition1, equalTo(partition2));
    }

}