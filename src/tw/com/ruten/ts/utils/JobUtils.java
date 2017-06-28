package tw.com.ruten.ts.utils;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.log4j.Logger;

public class JobUtils {
	public static Logger LOG = Logger.getLogger(JobUtils.class);
	
	public static boolean sumbitJob(org.apache.hadoop.mapreduce.Job job, 
			boolean verbose) throws IOException {
		
		try{
			job.submit();
			JobID jobid = job.getJobID();
			
			LOG.info("submit job, jobid=" + jobid.toString() );
			
			if (verbose) {
				job.monitorAndPrintJob();
			} else {
				int completionPollIntervalMillis = org.apache.hadoop.mapreduce.Job.getCompletionPollInterval(job.getConfiguration());
				while (!job.isComplete())
				try {
					Thread.sleep(completionPollIntervalMillis);
				} catch (InterruptedException interruptedexception) { }
			}
		
		}catch (Exception e){
			
		}finally{

		}
		
		return job.isSuccessful();
	}
	
	public static boolean sumbitJob(org.apache.hadoop.mapreduce.Job job, 
			boolean verbose,
			Path... lockPath) throws IOException {
		
		FileSystem fs = FileSystem.get(job.getConfiguration());
		String applicationID = null;
		
		try{
			job.submit();
			JobID jobid = job.getJobID();
			applicationID = jobid.appendTo(new StringBuilder("application")).toString();
			
			LOG.info("submit job, jobid=" + jobid.toString() );
			for(Path path: lockPath){
				createLockFile(fs, new Path(path, applicationID + ".lock.file"), true);
			}
			
			if (verbose) {
				job.monitorAndPrintJob();
			} else {
				int completionPollIntervalMillis = org.apache.hadoop.mapreduce.Job.getCompletionPollInterval(job.getConfiguration());
				while (!job.isComplete())
				try {
					Thread.sleep(completionPollIntervalMillis);
				} catch (InterruptedException interruptedexception) { }
			}
		
		}catch (Exception e){
			
		}finally{
			if(applicationID != null){
				for(Path path: lockPath){
					removeLockFile(fs, new Path(path, applicationID + ".lock.file"));
				}
			}
		}
		
		return job.isSuccessful();
	}
	
	public static boolean sumbitJob(org.apache.hadoop.mapreduce.Job job, 
			boolean verbose,
			String... lockPath) throws IOException {
		
		FileSystem fs = FileSystem.get(job.getConfiguration());
		String applicationID = null;
		
		try{
			job.submit();
			JobID jobid = job.getJobID();
			applicationID = jobid.appendTo(new StringBuilder("application")).toString();
			
			LOG.info("submit job, jobid=" + jobid.toString() );
			for(String path: lockPath){
				createLockFile(fs, new Path(path, applicationID + ".lock.file"), true);
			}
			
			if (verbose) {
				job.monitorAndPrintJob();
			} else {
				int completionPollIntervalMillis = org.apache.hadoop.mapreduce.Job.getCompletionPollInterval(job.getConfiguration());
				while (!job.isComplete())
				try {
					Thread.sleep(completionPollIntervalMillis);
				} catch (InterruptedException interruptedexception) { }
			}
		
		}catch (Exception e){
			
		}finally{
			if(applicationID != null){
				for(String path: lockPath){
					removeLockFile(fs, new Path(path, applicationID + ".lock.file"));
				}
			}
		}
		
		return job.isSuccessful();
	}	
	
	  public static void createLockFile(FileSystem fs, Path lockFile, boolean accept)
		      throws IOException {
		    if (fs.exists(lockFile)) {
		      if (!accept)
		        throw new IOException("lock file " + lockFile + " already exists.");
		      if (fs.getFileStatus(lockFile).isDirectory())
		        throw new IOException("lock file " + lockFile
		            + " already exists and is a directory.");
		      // do nothing - the file already exists.
		    } else {
		      // make sure parents exist
		      fs.mkdirs(lockFile.getParent());
		      fs.createNewFile(lockFile);
		    }
		  }
	  public static boolean removeLockFile(FileSystem fs, Path lockFile)
		      throws IOException {
		    if (!fs.exists(lockFile))
		      return false;
		    if (fs.getFileStatus(lockFile).isDirectory())
		      throw new IOException("lock file " + lockFile
		          + " exists but is a directory!");
		    return fs.delete(lockFile, false);
		  }
}
