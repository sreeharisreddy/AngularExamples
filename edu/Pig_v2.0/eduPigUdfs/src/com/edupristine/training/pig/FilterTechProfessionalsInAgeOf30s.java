package com.edupristine.training.pig;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class FilterTechProfessionalsInAgeOf30s extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		String profession = input.get(3).toString();
		int age = Integer.parseInt(input.get(1).toString());
		boolean shouldFilter = false;
		if ((profession.contains("technician") || profession.contains("programmer") ||
				profession.contains("engineer")) && (age >= 30 && age < 40)) {
			shouldFilter = true;
		}
		return shouldFilter;
	}

}
