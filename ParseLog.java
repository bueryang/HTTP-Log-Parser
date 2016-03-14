import java.io.File;


public class ParseLog {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Parser parser = new Parser();
		parser.Parse(new File("UofS_access_log"));	}

}
