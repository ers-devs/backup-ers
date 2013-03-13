package edu.kit.aifb.cumulus.webapp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

public class TestRegex extends TestCase {
	public void testRegex() {
		Pattern pat = Pattern.compile("/r/");
		
		String input = "/r/1";
		String output = input;
		
		Matcher mat = pat.matcher("/r/1");
		if (mat.find()) {
			output = mat.replaceAll("/d/");
		}

		System.out.println("in: " + input + " out: " + output);
	}
}
