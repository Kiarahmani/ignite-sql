// READY for GLOBAL test4
//
// CHOPPED
// CHOPPED
package com.mycompany.app;
import java.util.*;
import  java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.*;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.*;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.QueryCursor;
import java.util.stream.Collectors;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.CachePeekMode;
import java.util.concurrent.ThreadLocalRandom;
import java.util.UUID;

// TABLES
class DoubleKey implements Comparable<DoubleKey>{
  public int k1;
  public int k2;
  public DoubleKey(int k1, int k2){
  	this.k1=k1;
	this.k2=k2;	
  }
  public int compareTo(DoubleKey other) {
        if(k1==other.k1)
					return (k2>other.k2) ? 1 : 0;
				else
					return (k1>other.k1) ? 1 : 0;
				
  }
  public boolean equals(DoubleKey other) {
				return (k1==other.k1&&k2==other.k2);
  }
}
	
class Student { 
  public String name; 
  public int age; 
  public String gender; 
  public int coid; 
  public boolean isAlive;
  public Student(String name, int age, String gender, int coid, boolean isAlive) { 
		this.isAlive = isAlive;
    this.name = name;
    this.age = age;
    this.gender = gender;
    this.coid = coid;
  } 
} 

class Instructor { 
  public String name; 
  public int age; 
  public String specialty; 
  public boolean isAlive;
  public Instructor(String name, int age, String specialty, boolean isAlive) {  
		this.isAlive = isAlive;
    this.name = name;
    this.age = age;
    this.specialty = specialty;
  } 
} 

class Transcript { 
  public int grade; 
  public int iid; 
  public boolean isAlive;
  public Transcript(int grade, int  iid, boolean isAlive) { 
		this.isAlive = isAlive;
    this.grade = grade;
    this.iid = iid;
  } 
} 

class College {
  public int id;
  public String name;
  public String founded;
  public int st_count;
  public boolean isAlive;
  public College(String name, String founded, int st_count, boolean isAlive) {
		this.isAlive = isAlive;
    this.name = name;
    this.founded = founded;
    this.st_count = st_count;
  }
}

class Course {
  public String title;
  public int coid;
  public int iid;
  public int credit;
  public int capacity;
  public boolean isAlive;
  public Course(String title, int coid, int iid, int credit, int capacity, boolean isAlive) {
		this.isAlive = isAlive;
    this.title = title;
    this.coid = coid;
    this.iid = iid;
    this.credit = credit;
    this.capacity = capacity;
  }
}

class Register {
  public String regdate;
  public boolean isAlive;
  public Register (String regdate, boolean isAlive){
		this.isAlive = isAlive;
    this.regdate = regdate;
  }
}

class ConsoleColors {
    // Reset
    public static final String RESET = "\033[0m";  // Text Reset
    // Regular Colors
    public static final String RED = "\033[0;31m";     // RED
    public static final String GREEN = "\033[0;32m";   // GREEN
    public static final String YELLOW = "\033[0;33m";  // YELLOW
    public static final String BLUE = "\033[0;34m";    // BLUE
    public static final String PURPLE = "\033[0;35m";  // PURPLE
    public static final String CYAN = "\033[0;36m";    // CYAN
    public static final String WHITE = "\033[0;37m";   // WHITE
}


//////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////
public class App 
{
    public static final int _ROUNDS = 1;
    public static final boolean _CHOPPED = true;
    public static final boolean _MASTER = true;
    public static final int _CLIENT_NUMBER = 512;
    public static final int _STUDENT_COUNT = 8;
    public static final int _INSTRUCTOR_COUNT = 10;
    public static final int _COLLEGE_COUNT = 5;
    public static final int _COURSE_COUNT = 4;
    public static final int _LAT_THRESHOLD = 2000;
    public static final int _TRANSCRIPT_COUNT = _STUDENT_COUNT*_COURSE_COUNT;
    public static final int _REGISTER_COUNT = _STUDENT_COUNT*_COURSE_COUNT;
    public static final int _TRIAL = 6;
    //ISOL
	 public static final TransactionIsolation _ISOLATION_LEVEL = TransactionIsolation.SERIALIZABLE;
    //public static final TransactionIsolation _ISOLATION_LEVEL_OPTIMAL=TransactionIsolation.READ_COMMITTED;
    public static final TransactionIsolation _ISOLATION_LEVEL_OPTIMAL = TransactionIsolation.SERIALIZABLE;
    static long[] myArray = new long[_CLIENT_NUMBER*_ROUNDS];
    private static AtomicLongArray at = new AtomicLongArray(myArray);
    public static void print(String s){
    	System.out.print(s);
    }


  public static void waitForStart(Ignite ignite){
        IgniteCache<Integer, Integer> cache_sync = ignite.cache("sync");
                System.out.println(">>>> Waiting for master to start...");
		Integer i = cache_sync.get(1);
		do{
			i = cache_sync.get(1);
                        try{Thread.sleep(500);}catch(Exception e){}
                	System.out.println(">");
		}while (i==null);
                System.out.println("<<<< Waiting for master's command...");
		do{
			i = cache_sync.get(1);
                        try{Thread.sleep(500);}catch(Exception e){}
                	System.out.println("<");
		}while (i==0);

  }

  public static boolean shouldInit (Ignite ignite){
  	IgniteCache<Integer, Student> cache_student = ignite.cache("student");
	HashSet<Integer> test_keys = new HashSet<Integer>();
	try{Thread.sleep(5000);}catch(Exception e){}
	return _MASTER;
  }


  public static Set<DoubleKey> initialize (Ignite ignite){
		IgniteCache<Integer, Student> cache_student = ignite.cache("student");
		IgniteCache<Integer, Course> cache_course = ignite.cache("course");
		IgniteCache<Integer, Instructor> cache_instuctor = ignite.cache("instructor");
		IgniteCache<DoubleKey, Transcript> cache_transcript = ignite.cache("transcript");
		IgniteCache<Integer, College> cache_college = ignite.cache("college");
		IgniteCache<DoubleKey, Register> cache_register = ignite.cache("register");
	
		HashSet<DoubleKey> all_double_keys = new HashSet<DoubleKey>();
		if (shouldInit(ignite)) 
		{
		for (int i=0;i<_STUDENT_COUNT;i++){
				   print("s");
					if (i<_STUDENT_COUNT/2){
						int coid = ThreadLocalRandom.current().nextInt(0, _COLLEGE_COUNT + 1);
						int age = ThreadLocalRandom.current().nextInt(17, 60);
						String name = UUID.randomUUID().toString();
						String gender = UUID.randomUUID().toString();
						cache_student.put(i,new Student(name,age,gender,coid,true));
					}
					else{
						cache_student.put(i,new Student("",0,"",0,false));
					}
		}
		print("initial students inserted\n");
		for (int i=0;i<_COLLEGE_COUNT;i++){
				  		print("c");
						int st_count = ThreadLocalRandom.current().nextInt(100, 500);
						String name = UUID.randomUUID().toString();
						String founded = UUID.randomUUID().toString();
						cache_college.put(i,new College(name,founded,st_count,true));
		}
		print("initial colleges inserted\n");
		for (int i=0;i<_INSTRUCTOR_COUNT;i++){
				  		print("i");
						int age = ThreadLocalRandom.current().nextInt(24, 80);
						String name = UUID.randomUUID().toString();
						String specialty = UUID.randomUUID().toString();
						cache_instuctor.put(i,new Instructor(name,age,specialty,true));
		}
		print("initial instructors inserted\n");
		for (int i=0;i<_COURSE_COUNT;i++){
				  	print("cr");
					if (i<_COURSE_COUNT/2){
						int coid = ThreadLocalRandom.current().nextInt(0, _COLLEGE_COUNT + 1);
						int iid = ThreadLocalRandom.current().nextInt(0, _INSTRUCTOR_COUNT + 1);
						int credit = ThreadLocalRandom.current().nextInt(1, 5);
						int capacity = ThreadLocalRandom.current().nextInt(10, 200);
						String title = UUID.randomUUID().toString();
						cache_course.put(i,new Course(title,coid,iid,credit,capacity,true));
					}
					else{
						cache_course.put(i,new Course("",0,0,0,0,false));
					}
		}
		print("initial courses inserted\n");
		for (int i=0;i<_STUDENT_COUNT;i++){
					for(int j=0;j<_COURSE_COUNT;j++){
						System.out.print("r"+i+"-"+j+" ");
						String regdate = UUID.randomUUID().toString();
						cache_register.put(new DoubleKey(i,j),new Register(regdate,false));
					}
		}
	print("initial registrations inserted\n");
	for (int i=0;i<_STUDENT_COUNT;i++){
					for(int j=0;j<_COURSE_COUNT;j++){
						System.out.print("t"+i+"-"+j+" ");
						cache_transcript.put(new DoubleKey(i,j),new Transcript(0,0,false));
						all_double_keys.add(new DoubleKey(i,j));
					}
	}
	print("initial transcripts inserted\n");

	}
	else{
		print("cache is already initialized\n");
		for (int i=0;i<_STUDENT_COUNT;i++){
					for(int j=0;j<_COURSE_COUNT;j++){
						System.out.print("t"+i+"-"+j+" ");
						all_double_keys.add(new DoubleKey(i,j));
					}
	}


	}
	return all_double_keys;
}





////////////// ENROLL STUDENT TRANSACTION
	public static long enroll_student1(int iter,long startTime, Ignite ignite){
			long timePassed = 0;
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL_OPTIMAL)) {
				int coid = ThreadLocalRandom.current().nextInt(0, _COLLEGE_COUNT);
				int age = ThreadLocalRandom.current().nextInt(17, 60);
				int st_id = ThreadLocalRandom.current().nextInt(0, _STUDENT_COUNT);
				String name = UUID.randomUUID().toString();
				String gender = UUID.randomUUID().toString();
				IgniteCache<Integer, Student> cache_student = ignite.cache("student");
				cache_student.put(st_id,new Student(name,age,gender,coid,true));
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
	public static long enroll_student2(int iter,long startTime, Ignite ignite){
			long timePassed = 0;
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL)) {
				IgniteCache<Integer, College> cache_college = ignite.cache("college");
				int coid = ThreadLocalRandom.current().nextInt(0, _COLLEGE_COUNT); //TODO: the coid must be passed from enroll_student1 (it's okay for testing time)
				College old_col = cache_college.get(coid);
 				cache_college.put (coid,new College(old_col.name,old_col.founded,old_col.st_count+1,true));
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
	  public static long enroll_student(int iter,long startTime, Ignite ignite){
                        long timePassed = 0;
                        try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL)) {
                                int coid = ThreadLocalRandom.current().nextInt(0, _COLLEGE_COUNT);
                                int age = ThreadLocalRandom.current().nextInt(17, 60);
                                int st_id = ThreadLocalRandom.current().nextInt(0, _STUDENT_COUNT);
                                String name = UUID.randomUUID().toString();
                                String gender = UUID.randomUUID().toString();
                                IgniteCache<Integer, Student> cache_student = ignite.cache("student");
                                IgniteCache<Integer, College> cache_college = ignite.cache("college");
                                cache_student.put(st_id,new Student(name,age,gender,coid,true));
                                College old_col = cache_college.get(coid);
                                cache_college.put (coid,new College(old_col.name,old_col.founded,old_col.st_count+1,true));
                                tx.commit();
                        }catch(TransactionOptimisticException e){}
                        return (System.currentTimeMillis() - startTime);
        }
//////////////////////////////////////////////////////////////////////



////////////// QUERY STUDENT TRANSACTION
	public static long query_student(int iter,long startTime, Ignite ignite, Set<DoubleKey> all_keys){
			long timePassed = 0;
			IgniteCache<Integer, Student> cache_student = ignite.cache("student");
			IgniteCache<Integer, Course> cache_course = ignite.cache("course");
			IgniteCache<Integer, College> cache_college = ignite.cache("college");
			IgniteCache<DoubleKey, Register> cache_register = ignite.cache("register");
		  IgniteCache<DoubleKey, Transcript> cache_transcript = ignite.cache("transcript");
			//print ("query student\n");
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL_OPTIMAL)) {
				int st_id = ThreadLocalRandom.current().nextInt(0, _STUDENT_COUNT);
				Student student = cache_student.get(st_id);
				if (student.isAlive){
					Set<DoubleKey> filteredKeys = all_keys.stream()
									    .filter(k -> k.k1 == st_id).collect(Collectors.toSet());
					College college = cache_college.get(student.coid);	
					// to avoid deadlocks
				  Set<DoubleKey> hset = new TreeSet<DoubleKey>();
					for (DoubleKey k:filteredKeys)
						hset.add(k);
					Map <DoubleKey,Register> regMap = cache_register.getAll(hset);
					Map <DoubleKey,Transcript> transMap = cache_transcript.getAll(hset);
					// query course that the student is registered 
					HashSet<Integer> courseSet = new HashSet<Integer>();
					for (DoubleKey k:(regMap.keySet())){
						if(regMap.get(k).isAlive)
										courseSet.add(k.k2);
					}
					// to avoid deadlocks
				  Set<Integer> hset2 = new TreeSet<Integer>();
					for (Integer k:courseSet)
						hset2.add(k);
					Map<Integer,Course> courses = cache_course.getAll(hset2);
				}
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
//////////////////////////////////////////////////////////////////////


//INSERT COURSE TRANSACTION
	public static long add_course(int iter,long startTime, Ignite ignite){
			long timePassed = 0;
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL_OPTIMAL)) {
				int cid = ThreadLocalRandom.current().nextInt(0, _COURSE_COUNT);
				int coid = ThreadLocalRandom.current().nextInt(0, _COLLEGE_COUNT);
				int iid = ThreadLocalRandom.current().nextInt(0, _INSTRUCTOR_COUNT);
				int credit = ThreadLocalRandom.current().nextInt(1, 4);
				int cap = ThreadLocalRandom.current().nextInt(5, 140);
				String title = UUID.randomUUID().toString();
				IgniteCache<Integer, Course> cache_course = ignite.cache("course");
				cache_course.put(cid,new Course(title,coid,iid,credit,cap,true));
				
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
//////////////////////////////////////////////////////////////////////



////////////// REMOVE COURSE TRANSACTION
	public static long remove_course(int iter,long startTime, Ignite ignite){
			long timePassed = 0;
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL_OPTIMAL)) {
				int cid = ThreadLocalRandom.current().nextInt(0, _COURSE_COUNT);
				IgniteCache<Integer, Course> cache_course = ignite.cache("course");
				cache_course.put(cid,new Course("",0,0,0,0,false));
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
//////////////////////////////////////////////////////////////////////
	

////////////// REGISTER COURSE TRANSACTION
	public static int register_course1(int iter,long startTime, Ignite ignite){
			long timePassed = 0;
			Course course;
			int cid = ThreadLocalRandom.current().nextInt(0, _COURSE_COUNT);
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL_OPTIMAL)) {
				int sid = ThreadLocalRandom.current().nextInt(0, _STUDENT_COUNT);
				IgniteCache<DoubleKey, Register> cache_register = ignite.cache("register");
				String today = "8/1/2018";
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return cid;
	}
	public static long register_course2(int cid, int iter,long startTime, Ignite ignite){
			long timePassed = 0;
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL)) {
				IgniteCache<Integer, Course> cache_course = ignite.cache("course");
				Course course = cache_course.get(cid);
				if (course.isAlive&&course.capacity>0 )
					cache_course.put(cid,new Course(course.title,course.coid,course.iid,course.credit,course.capacity-1,true));
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
        public static long register_course(int iter,long startTime, Ignite ignite){
                        long timePassed = 0;
                        try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL)) {
                                int cid = ThreadLocalRandom.current().nextInt(0, _COURSE_COUNT);
                                int sid = ThreadLocalRandom.current().nextInt(0, _STUDENT_COUNT);
                                IgniteCache<Integer, Course> cache_course = ignite.cache("course");
                                IgniteCache<DoubleKey, Register> cache_register = ignite.cache("register");
                                String today = "8/1/2018";
                                Course course = cache_course.get(cid);
                                if (course.capacity>0 && course.isAlive){
                                        cache_course.put(cid,new Course(course.title,course.coid,course.iid,course.credit,course.capacity-1,true));
                                }
                                tx.commit();
                        }catch(TransactionOptimisticException e){}
                        return (System.currentTimeMillis() - startTime);
        }

//////////////////////////////////////////////////////////////////////


////////////// QUERY COURSE TRANSACTION
	public static long query_course(int iter,long startTime, Ignite ignite, Set<DoubleKey> all_keys){
			long timePassed = 0;
			IgniteCache<Integer, Student> cache_student = ignite.cache("student");
			IgniteCache<Integer, Course> cache_course = ignite.cache("course");
			IgniteCache<DoubleKey, Register> cache_register = ignite.cache("register");
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL_OPTIMAL)) {
				int cid = ThreadLocalRandom.current().nextInt(0, _COURSE_COUNT);
				Course course = cache_course.get(cid);
				if (course.isAlive){
					Set<DoubleKey> filteredKeys = all_keys.stream()
									    .filter(k -> k.k2 == cid).collect(Collectors.toSet());
					// to avoid deadlocks
					Set<DoubleKey> hset = new TreeSet<DoubleKey>();
					for (DoubleKey k:filteredKeys)
						hset.add(k);
					Map <DoubleKey,Register> regMap = cache_register.getAll(hset);
					
					// query students that are registered for this course
					HashSet<Integer> studentSet = new HashSet<Integer>();
					for (DoubleKey k:(regMap.keySet())){
						if(regMap.get(k).isAlive)
										studentSet.add(k.k1);
					}
					Map<Integer,Student> students = cache_student.getAll(studentSet);
					//System.out.println(students.size());
				}
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
//////////////////////////////////////////////////////////////////////

////////////// INCREASE CAPACITY TRANSACTION
	public static long increase_capacity(int iter,long startTime, Ignite ignite){
			long timePassed = 0;
			IgniteCache<Integer, College> cache_college = ignite.cache("college");
			IgniteCache<Integer, Course> cache_course = ignite.cache("course");
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL)) {
				Set<Integer> colSetKeys = new HashSet<Integer>();
				// all colleges with more than 300 students
				for (int i=0;i<_COLLEGE_COUNT;i++)
								colSetKeys.add(i);
				// to avoid deadlocks
				Set<Integer> hset = new TreeSet<Integer>();
				for (Integer k:colSetKeys)
					hset.add(k);
				Map <Integer,College> colMap = cache_college.getAll(hset);		
				Set<Integer> filteredKeys = colSetKeys.stream()
									    .filter(k -> colMap.get(k).st_count > 300).collect(Collectors.toSet());
				// all courses offered at any of the above colleges
				Set<Integer> courseSetKeys = new HashSet<Integer>();
				for (int i=0;i<_COURSE_COUNT;i++)
								courseSetKeys.add(i);
				// to avoid deadlocks
				Set<Integer> hset2 = new TreeSet<Integer>();
				for (Integer k:courseSetKeys)
					hset2.add(k);
				Map <Integer,Course> courseMap = cache_course.getAll(hset2);		
				Set<Integer> filteredCourseKeys = new HashSet<Integer>();
				for(Integer id:filteredKeys)
					filteredCourseKeys =  courseSetKeys.stream()
										    .filter(k -> courseMap.get(k).coid == id).collect(Collectors.toSet());
				// update courses capacities
				for(Integer cid:filteredCourseKeys){
								Course course = courseMap.get(cid);
								cache_course.put(cid,new Course(course.title,course.coid,course.iid,course.credit,course.capacity+20,true));
				}

				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
//////////////////////////////////////////////////////////////////////


////////////// EXPEL STUDENT TRANSACTION
	public static long expel_student(int iter,long startTime, Ignite ignite,Set<DoubleKey> all_keys){
			long timePassed = 0;
			IgniteCache<Integer, Student> cache_student = ignite.cache("student");
		  IgniteCache<DoubleKey, Transcript> cache_transcript = ignite.cache("transcript");
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL)) {
				
				// to avoid deadlocks
				Set<DoubleKey> hset = new TreeSet<DoubleKey>();
				for (DoubleKey k:all_keys)
					hset.add(k);
				// query all transcripts
				Map <DoubleKey,Transcript> transMap = cache_transcript.getAll(hset);
				//return the list of students to be expelled
				Map<Integer,Student> filteredStudentKeyMap = new HashMap<Integer,Student>();
				for (int i=0;i<_STUDENT_COUNT;i++){
					final int ii = i;
					Set<DoubleKey> filteredDoubleKeys = transMap.keySet().stream()
									    .filter(k -> k.k1 == ii).collect(Collectors.toSet());
					if (filteredDoubleKeys.size()>3)
									filteredStudentKeyMap.put(i,new Student("",0,"",0,false));
				}
				// kill the student
				cache_student.putAll(filteredStudentKeyMap);
				// clear their registrations
				// TODO
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
//////////////////////////////////////////////////////////////////////


////////////// ENTER GRADE TRANSACTION
	public static long enter_grade(int iter,long startTime, Ignite ignite){
			long timePassed = 0;
		  IgniteCache<DoubleKey, Transcript> cache_transcript = ignite.cache("transcript");
			try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, _ISOLATION_LEVEL_OPTIMAL)) {
				int st_id = ThreadLocalRandom.current().nextInt(0, _STUDENT_COUNT);
				int c_id = ThreadLocalRandom.current().nextInt(0, _COURSE_COUNT);
				int i_id = ThreadLocalRandom.current().nextInt(0, _INSTRUCTOR_COUNT);
				int grade = ThreadLocalRandom.current().nextInt(0, 100);
				cache_transcript.put(new DoubleKey(st_id,c_id),new Transcript(grade,i_id,true));
				
				tx.commit();
			}catch(TransactionOptimisticException e){}
			return (System.currentTimeMillis() - startTime);
	}
//////////////////////////////////////////////////////////////////////





  
	public static void main(String[] args) {
        // CACHE INITIALIZATION
        double sum=0;
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start("./test_client.xml");
        Set<DoubleKey> all_keys;
				/////////////////////
        // IF MASTER:
				if(_MASTER){
        	IgniteCache<Integer, Integer> cache_sync = ignite.cache("sync");
        	cache_sync.put(1,0);
        	all_keys = initialize (ignite);
        	System.out.println ("Initial rows inserted");
	        cache_sync.put(1,1);
					try{Thread.sleep(500);}catch(Exception e){}
				}else{
        	all_keys = initialize (ignite);
        	waitForStart(ignite);
				}


	// CLIENTS TASKS
	Runnable r = new Runnable(){
		@Override
		public void run(){
			try{
				int threadId = (int) (Thread.currentThread().getId()%_CLIENT_NUMBER);
				//System.out.print (" Client started:"+threadId);
				for (int i=0;i<_ROUNDS;i++){
					int txn_type_rand = ThreadLocalRandom.current().nextInt(0, 100);
					long startTime = System.currentTimeMillis();
					long estimatedTime = 1010101010;
					String color=ConsoleColors.RESET;
					if (txn_type_rand<10){
						if(_CHOPPED){
							enroll_student1 (_TRIAL,startTime,ignite);
							enroll_student2 (_TRIAL,startTime,ignite);
						}else
							enroll_student (_TRIAL,startTime,ignite);
						estimatedTime=System.currentTimeMillis() - startTime;
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Enroll_Student    ("+estimatedTime+"ms)");
					}
					if (10<=txn_type_rand && txn_type_rand<30){
						estimatedTime = query_student (_TRIAL,startTime,ignite,all_keys);
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+ "Query_Student     ("+estimatedTime+"ms)");
					}
					if (30<=txn_type_rand && txn_type_rand<35){
						estimatedTime = add_course (_TRIAL,startTime,ignite);
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Add_Course        ("+estimatedTime+"ms)");
					}
					if (35<=txn_type_rand && txn_type_rand<40){
						estimatedTime = remove_course (_TRIAL,startTime,ignite);
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Remove_Course     ("+estimatedTime+"ms)");
					}
					if (40<=txn_type_rand && txn_type_rand<50){
						if(_CHOPPED)
							register_course2 (register_course1 (_TRIAL,startTime,ignite),_TRIAL,startTime,ignite);
						else
							register_course (_TRIAL,startTime,ignite);
						estimatedTime=System.currentTimeMillis() - startTime;
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Register_Course   ("+estimatedTime+"ms)");
					}
					if (50<=txn_type_rand && txn_type_rand<80){
						estimatedTime = query_course (_TRIAL,startTime,ignite,all_keys);
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Query_Course      ("+estimatedTime+"ms)");
					}
					if (80<=txn_type_rand && txn_type_rand<88){
						estimatedTime = increase_capacity (_TRIAL,startTime,ignite);
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Increase_Capacity ("+estimatedTime+"ms)");
					}
					if (88<=txn_type_rand && txn_type_rand<90){
						estimatedTime = expel_student (_TRIAL,startTime,ignite,all_keys);
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Expel_Student     ("+estimatedTime+"ms)");
					}
					if (90<=txn_type_rand && txn_type_rand<100){
						estimatedTime = enter_grade (_TRIAL,startTime,ignite);
						color = (estimatedTime>_LAT_THRESHOLD)? ConsoleColors.RED:ConsoleColors.RESET;
						System.out.println(color+"Enter_Grade       ("+estimatedTime+"ms)");
					}





					at.set(threadId*_ROUNDS+i,estimatedTime);
				}
			}
			catch (Exception e){
				System.out.println(e);
			}
		}
        };

	long startTime = System.currentTimeMillis();
	// INITIATE CONCURRENT CLIENTS
	Thread threads[] = new Thread[_CLIENT_NUMBER];
        for (int i=0; i<_CLIENT_NUMBER; i++){
                threads[i] = new Thread(r);
                threads[i].start();
        }
	// WAIT FOR ALL CLEINTS
        for (int i=0; i<_CLIENT_NUMBER; i++){
                try{
			threads[i].join();
		}catch(InterruptedException e){
			System.out.println(e);
		}
        }
	// PRINT STATS
	long estimatedTime_tp = System.currentTimeMillis() - startTime;
	long sum_time = 0;
	int failed=0;
        for (int i=0; i<_CLIENT_NUMBER*_ROUNDS; i++ ){
		if(at.get(i)!=1010101010)
			sum_time += at.get(i);
		else
			failed++;
	}
	System.out.println(ConsoleColors.RESET+"\n\n===============================");
	System.out.println("AVG TXN TIME: "+ sum_time/(_CLIENT_NUMBER*_ROUNDS-failed)+"ms");
	System.out.println("Throuput: "+ (_ROUNDS*_CLIENT_NUMBER-failed)*1000/estimatedTime_tp+" rounds/s");
	System.out.println("TOTAL RUNNING TIME: "+estimatedTime_tp/1000.0+"s");
	System.out.println("Failed Txns: "+failed*100.0/(_CLIENT_NUMBER*_ROUNDS)+"%");
	System.out.println("===============================\n\n\n");
    }
}











