import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.exp.*;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.*;
import com.aerospike.client.task.IndexTask;
import org.junit.Assert;
import org.junit.Test;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ExpressionsTest1 {

    String[] _firstName =  new String[] { "Adam", "Alex", "Aaron", "Ben", "Carl", "Dan", "David", "Edward", "Fred", "Frank", "George", "Hal", "Hank", "Rahul", "John", "Jack", "Joe", "Larry", "Monte", "Matthew", "Mark", "Nathan", "Otto", "Paul", "Peter", "Roger", "Roger", "Steve", "Thomas", "Tim", "Ty", "Victor", "Walter"};
    String[] _lastName = new String[] { "Anderson", "Ashwoon", "Aikin", "Bateman", "Bongard", "Bowers", "Boyd", "Cannon", "Cast", "Deitz", "Dewalt", "Ebner", "Frick", "Ra", "Haworth", "Hesch", "Hoffman", "Kassing", "Knutson", "Lawless", "Lawicki", "Mccord", "McCormack", "Miller", "Myers", "Nugent", "Ortiz", "Orwig", "Schlicht", "Schmitt", "Schwager", "Schutz", "Schuster"};
    String[] _reasonVales = new String[] {"overtaking on the inside","driving too close to another vehicle","driving through a red light by mistake","turning into the path of another vehicle","the driver being avoidably distracted by tuning the radio, lighting a cigarette etc","flashing lights to force other drivers to give way","misusing lanes to gain advantage over other drivers","unnecessarily staying in an overtaking lane","unnecessarily slow driving or braking","dazzling other drivers with un-dipped headlights","using a hand-held phone or other equipment","reading, or looking at a map","talking to and looking at a passenger","lighting a cigarette, selecting music, tuning the radio","speeding, racing, or driving aggressively","ignoring traffic lights, road signs or warnings from passengers","overtaking dangerously","driving under the influence of drink or drugs, including prescription drugs","driving when unfit or being sleepy","knowing the vehicle has a dangerous fault or an unsafe load","drive without a driving licence covering a vehicle of the class being driven","drive without insurance","drive without an MOT","drive while disqualified","Using a mobile phone or handheld device","Driving whilst under the influence of alcohol or drugs","sleeping at the wheel","wearing flip flops","Failing to stop after an accident","Failing to give particulars or report an accident within 24 hours","Undefined accident offences","Driving while disqualified by order of court","Attempting to drive while disqualified by order of court"};
    String [] _cities = new String[]{"Bath","Birmingham","Durham","Brighton & Hove","Durham","Cambridge","Canterbury","Carlisle","Chelmsford","Chester","Chichester","Colchester","Coventry","Northampton","Doncaster","Durham","Ely","Exeter","Gloucester","Hereford","Kingston-upon-Hull","Lancaster","Leeds","Leicester","Lichfield","Lincoln","Liverpool","London","Manchester","Milton Keynes","Newcastle-upon-Tyne","Norwich","Nottingham"};

    public AerospikeClient client;
    String namespace = "test";
    String set = "fines";

    @Test
    public void setUpConnection() throws Exception {
        client = new AerospikeClient(null, new Host("localhost", 3000));
        Assert.assertTrue(client.getCluster().isActive());
    }
    @Test
    public void insertData() throws Exception {
        setUpConnection();
        client.truncate(null,namespace,set,null);
        BatchPolicy bPolicy = new BatchPolicy(client.batchPolicyDefault);
        bPolicy.setTimeout(1000);

        List<BatchRecord> records = new ArrayList<>();

        for (int i=0; i<_firstName.length; i++) {

            Key key_one = new Key(namespace, set, i);
            Bin firstName = new Bin("firstname", Value.get(_firstName[i]));
            Bin lastname = new Bin("lastname", Value.get(_lastName[i]));
            Bin reason = new Bin("reason", _reasonVales[i]);
            Bin fine = new Bin("fine", Value.get(50f));
            Bin city = new Bin("city", Value.get(_cities[i]));

            Operation[] operations = Operation.array(
                    Operation.put(firstName),
                    Operation.put(lastname),
                    Operation.put(reason),
                    Operation.put(fine),
                    Operation.put(city)
            );
            records.add(new BatchWrite(key_one,operations));
        }
        boolean retval = client.operate(bPolicy,records);
        Assert.assertTrue(retval);
    }
    @Test
    public void getData() throws Exception {
        boolean retval = false;
        int expected_count =_lastName.length;
        setUpConnection();
        Key[] keys = new Key[_firstName.length];
        List<BatchRecord> batchRecords = new ArrayList<>();

        for (int i = 0; i < keys.length; i++) {
            keys[i] = new Key(namespace, set, (i));
            Operation[] operations = Operation.array(
                    Operation.get("lastname"),
                    Operation.get("fine"),
                    Operation.get("updatedFine"),
                    Operation.get("city")
            );
            batchRecords.add(new BatchRead(keys[i], operations));
        }

        BatchPolicy bPolicy = new BatchPolicy(client.batchPolicyDefault);
        bPolicy.respondAllKeys = false;              // note key-not-found does not stop batch execution
        try {
            boolean status = client.operate(bPolicy, batchRecords);
            if (status) { System.out.println("All batch operations succeeded.");}
            else { System.out.println("Some batch operations failed.");}
        }
        catch (AerospikeException e) {
            System.out.format("%s", e);
        }

        // Show results
        int j=0;
        for (int i = 0; i < keys.length; i++) {
            BatchRecord batchRec = batchRecords.get(i);
            Record rec = batchRec.record;
            Key key = batchRec.key;
            if (batchRec.resultCode == ResultCode.OK) {
                Object v1 = rec.getValue("lastname");
                Object v2 = rec.getValue("fine");
                Object v4 = rec.getValue("city");
                Object v3 = rec.getValue("updatedFine");
                System.out.format("[Key:%s,ns:%s,set:%s], Lastname: %s, Fine: %s, Updated-Fine: %s, City: %s\n",
                        key.userKey, key.namespace, key.setName, v1, v2, v3, v4);
                j++;
            } else {
                System.out.format("Result[%d]: error: %s\n", i, ResultCode.getResultString(batchRec.resultCode));
            }
        }
        Assert.assertEquals(expected_count,j);
    }
    @Test
    public void updatePenaltyBatch() throws Exception {
        setUpConnection();
        List<BatchRecord> batchRecords = new ArrayList<>();
        BatchPolicy batchPolicy = new BatchPolicy(client.writePolicyDefault);

        for (int i = 0; i < _lastName.length; i++) {
            Expression increaseFineGeoExp = Exp.build(
                    Exp.add(
                            Exp.cond(
                                    Exp.binExists("updatedFine"), Exp.floatBin("updatedFine"),
                                    Exp.floatBin("fine")
                            ),
                            Exp.mul(
                                    Exp.floatBin("fine"),
                                    Exp.cond(
                                            Exp.or(
                                                    Exp.eq(Exp.stringBin("city"), Exp.val("Ely")),
                                                    Exp.eq(Exp.stringBin("city"), Exp.val("Chelmsford")),
                                                    Exp.eq(Exp.stringBin("city"), Exp.val("Brighton & Hove"))
                                            ),
                                            Exp.val(1.5f),
                                            Exp.val(0f)
                                    )
                            )
                    )
            );
            Expression increaseFineByNameExp = Exp.build(
                    Exp.mul(
                            Exp.cond(
                                    Exp.binExists("updatedFine"), Exp.floatBin("updatedFine"),
                                    Exp.floatBin("fine")
                            ),
                            Exp.cond(
                                    Exp.eq( Exp.stringBin("lastname"), Exp.val("Hoffman")),
                                    Exp.val(3f),
                                    Exp.val(1f)
                            )
                    )
            );
            MyDate rnd = randomDate(7);

            Expression paidDateExp = Exp.build(
                    Exp.cond(
                            Exp.binExists("paid"), Exp.intBin("paid"),
                            Exp.val( rnd.getDateLong() )
                    )
            );

            Expression paidDateHumanExp = Exp.build(
                    Exp.cond(
                            Exp.binExists("paid_h"), Exp.stringBin("paid_h"),
                            Exp.val( rnd.getHumanReadableDate() )
                    )
            );

            Operation[] ops = Operation.array(
                    ExpOperation.write("updatedFine", increaseFineGeoExp, ExpWriteFlags.DEFAULT),
                    ExpOperation.write("updatedFine", increaseFineByNameExp, ExpWriteFlags.DEFAULT),
                    ExpOperation.write("paid", paidDateExp, ExpWriteFlags.DEFAULT),
                    ExpOperation.write("paid_h", paidDateHumanExp, ExpWriteFlags.DEFAULT)
            );

            batchRecords.add(new BatchWrite( new Key(namespace, set,i), ops));
        }

        try {
            client.operate(batchPolicy, batchRecords);
        }
        catch (AerospikeException e) {
            System.out.format("%s\n", e);
            e.printStackTrace();
        }
    }
    @Test
    public void flipFlopSagaDiscount() throws Exception {
        setUpConnection();
        List<BatchRecord> batchRecords = new ArrayList<>();
        BatchPolicy batchPolicy = new BatchPolicy(client.writePolicyDefault);

        for (int i = 0; i < _lastName.length; i++) {
            Expression flipFlopDiscountExp = Exp.build(
                    Exp.sub( Exp.cond(
                            Exp.binExists("updatedFine"), Exp.floatBin("updatedFine"),
                            Exp.floatBin("fine")
                            ),
                            Exp.mul(
                                    Exp.cond(
                                            Exp.regexCompare(".*flops.*", RegexFlag.ICASE, Exp.stringBin("reason")),
                                            Exp.max(
                                                    Exp.floatBin("fine"),
                                                    Exp.cond(
                                                            Exp.binExists("updatedFine"), Exp.floatBin("updatedFine"),
                                                            Exp.val(-1f) // in case fine is zero
                                                    )
                                            ), // with flip flops
                                            Exp.val(0f) // else no flip flops
                                    ),
                                    Exp.val(0.5f)
                            )
                    )
            );

            Operation[] ops = Operation.array(
                    ExpOperation.write("updatedFine", flipFlopDiscountExp, ExpWriteFlags.DEFAULT),
                    Operation.get("firstname"),
                    Operation.get("lastname"),
                    Operation.get("reason"),
                    Operation.get("updatedFine")
            );

            batchRecords.add(new BatchWrite( new Key(namespace, set,i), ops));
        }
        try {
            client.operate(batchPolicy, batchRecords);
        }
        catch (AerospikeException e) {
            System.out.format("%s\n", e);
            e.printStackTrace();
        }

        // Show results
        Iterator<BatchRecord> itr = batchRecords.iterator();
        int j=0;
        int expected_count =_lastName.length;
        while ( itr.hasNext() )
        {
            BatchRecord batchRec = itr.next();
            if (batchRec.resultCode == ResultCode.OK) {
                j++;
                Record rec = batchRec.record;
                Key key = batchRec.key;
                Object v1 = rec.getValue("firstname");
                Object v2 = rec.getValue("lastname");
                Object v3 = rec.getValue("updatedFine");
                Object v4 = rec.getValue("reason");
                System.out.format("FirstName: %s, Lastname: %s, Updated-Fine: %s, Reason: %s\n",
                        v1, v2, v3, v4);
            }
            else {
                System.out.format("Result[%d]: error: %s\n", j, ResultCode.getResultString(batchRec.resultCode));
            }
        }
        Assert.assertEquals(expected_count,j);
    }
    @Test
    public void getDataByCityByTimeRange() throws Exception {
        setUpConnection();
        createIndexOn_Paid();

        // Create new query policy
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.filterExp = Exp.build(
                Exp.regexCompare("^c[ah].*", RegexFlag.ICASE, Exp.stringBin("city"))
        );

        // do not include record bins
        queryPolicy.includeBinData = false;

        // Create statement
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);

        // Create index filter
        long fromTime_ = getDate( randomDate(7).formatDate );
        long toTime_ = getDate( randomDate(4).formatDate );
        if ( fromTime_ > toTime_ )
        {
            long tmp = fromTime_;
            fromTime_ = toTime_;
            toTime_ = tmp;
        }
        stmt.setFilter(Filter.range("paid", fromTime_, toTime_));

        // Execute the query
        RecordSet recordSet = client.query(queryPolicy, stmt);

        // Our BATCH query
        List<BatchRecord> batchRecords = new ArrayList<>();
        Operation[] ops1 = Operation.array(
                Operation.get("firstname"), Operation.get("lastname"),
                Operation.get("updatedFine"), Operation.get("city"),
                Operation.get("paid_h")
        );
        try{
            while(recordSet.next()){
                batchRecords.add(new BatchRead(recordSet.getKey(), ops1));
            }
        }
        finally{
            recordSet.close();
        }

        // Get some date from a  batch query
        BatchPolicy bPolicy = new BatchPolicy(client.batchPolicyDefault);
        bPolicy.respondAllKeys = false;              // note key-not-found does not stop batch execution
        try {
            boolean status = client.operate(bPolicy, batchRecords);
            if (status) { System.out.println("All batch operations succeeded.");}
            else { System.out.println("Some batch operations failed.");}
        }
        catch (AerospikeException e) {
            System.out.format("%s", e);
        }

        // Show results
        int j=0;
        for (int i = 0; i < batchRecords.size(); i++) {
            BatchRecord batchRec = batchRecords.get(i);
            Record rec = batchRec.record;
            Key key = batchRec.key;
            if (batchRec.resultCode == ResultCode.OK) {
                Object v1 = rec.getValue("firstname");
                Object v2 = rec.getValue("lastname");
                Object v3 = rec.getValue("updatedFine");
                Object v4 = rec.getValue("city");
                Object v5 = rec.getValue("paid_h");
                System.out.format("%s - City: %s, FirstName: %s, Lastname: %s, Updated-Fine: %s\n",
                        v5, v4, v1, v2, v3);
                j++;
            } else {
                System.out.format("Result[%d]: error: %s\n", i, ResultCode.getResultString(batchRec.resultCode));
            }
        }
        client.close();
        Assert.assertEquals(batchRecords.size(), batchRecords.size());

    }
    @Test
    public void addPenaltyByRecord() throws Exception {
        setUpConnection();

        WritePolicy writePolicy = new WritePolicy();

        Expression increaseFineExp = Exp.build(
                Exp.mul(
                        Exp.floatBin("fine"),
                        Exp.cond(
                                Exp.eq( Exp.stringBin("lastname"), Exp.val("Hoffman")),
                                Exp.val(1.5d),
                                Exp.val(1d)
                        )
                )
        );
        for (int i = 0; i < _lastName.length; i++) {
            Record record = client.operate(
                    writePolicy,
                    new Key(namespace, set,i),
                    ExpOperation.write("fine", increaseFineExp, ExpWriteFlags.DEFAULT)
            );
        }
    }
    @Test
    public void createIndexOn_City() throws Exception {
        setUpConnection();
        // Create index task
        IndexTask task = client.createIndex(null,
                namespace, // namespace
                set, // set name
                "city_idx",
                "city",
                IndexType.STRING
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }
    @Test
    public void deleteIndexOn_City() throws Exception {
        setUpConnection();
        // Create index task
        IndexTask task = client.dropIndex(null,
                namespace, // namespace
                set, // set name
                "city_idx"
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }
    @Test
    public void createIndexOn_UpdatedFine() throws Exception {
        setUpConnection();
        // Create index task
        IndexTask task = client.createIndex(null,
                namespace, // namespace
                set, // set name
                "updatedFine_idx",
                "updatedFine",
                IndexType.NUMERIC
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }
    @Test
    public void deleteIndex_UpdatedFine() throws Exception {
        setUpConnection();
        // Create index task
        IndexTask task = client.dropIndex(null,
                namespace, // namespace
                set, // set name
                "updatedFine_idx"
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }
    @Test
    public void createIndexOn_Paid() throws Exception {
        setUpConnection();
        // Create index task
        IndexTask task = client.createIndex(null,
                namespace, // namespace
                set, // set name
                "paid_idx",
                "paid",
                IndexType.NUMERIC
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }
    @Test
    public void deleteIndexOn_Paid() throws Exception {
        setUpConnection();
        // Create index task
        IndexTask task = client.dropIndex(null,
                namespace, // namespace
                set, // set name,
                "paid_idx"
        );
        // Wait for the task to complete
        task.waitTillComplete();
        int status = task.queryStatus();
        Assert.assertEquals(2, status);
    }
    //@Test
    public MyDate randomDate(int variant) {
        // Over the past week
        long date = System.currentTimeMillis()-(new Random().nextInt(86400000 * variant));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date d = new Date(date);
        String formatDate = sdf.format(d);
        return new MyDate(date, formatDate);
    }
    public long getDate(String date_ing) {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        try {
            Date d = f.parse(date_ing);
            long milliseconds = d.getTime();
            return milliseconds;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }
    class MyDate {
        private final long d;
        private final String formatDate;

        MyDate(long d, String formatDate)
        {
            this.d = d;
            this.formatDate = formatDate;
        }

        private long getDateLong(){
            return d;
        }

        private String getHumanReadableDate(){
            return formatDate;
        }
    }
}