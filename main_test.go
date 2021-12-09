package main

/*func TestGetSubjectsFromQuery(t *testing.T) {

	//common case
	var (
		query    = "subject=2820;2821;3967"
		expected = []string{
			"2820",
			"2821",
			"3967",
		}
		expectedFilters   = []string{}
		subjects, filters = getSubjectsFromQuery(query)
		l1                = len(expected)
		l2                = len(subjects)
	)

	if l1 != l2 {
		t.Errorf("expected %v != result %v by length", expected, subjects)
	} else if len(filters) > 0 {
		t.Errorf("expected %v must be not elastic", expected)
	}

	for i := range subjects {
		if expected[i] != subjects[i] {
			t.Errorf("expected %v != result %v by value:\n%s != %s",
				expected,
				subjects,
				expected[i],
				subjects[i])
		}
	}

	//elastic/not-elastic check filters case
	query = "subject=341;375;382&ext=66599;456"
	expected = []string{
		"341",
		"375",
		"382",
	}
	expectedFilters = []string{
		"66599",
		"456",
	}
	subjects, filters = getSubjectsFromQuery(query)
	l1 = len(expected)
	l2 = len(subjects)

	if l1 != l2 {
		t.Errorf("expected %v != result %v by length", expected, subjects)
	} else if len(filters) == 0 {
		t.Errorf("expected %v must be elastic", expected)
	} else if len(filters) != 2 {
		t.Errorf("incorrect filters: %v != [66599 456]", filters)
	}

	for i := range expectedFilters {
		if expectedFilters[i] != filters[i] {
			t.Errorf("expected filters %v != result filters %v by value:\n%s != %s",
				expectedFilters,
				filters,
				expectedFilters[i],
				filters[i])
		}
	}

	for i := range subjects {
		if expected[i] != subjects[i] {
			t.Errorf("expected %v != result %v by value:\n%s != %s",
				expected,
				subjects,
				expected[i],
				subjects[i])
		}
	}

	//empty string case
	query = ""
	expected = []string{}
	expectedFilters = []string{}
	subjects, filters = getSubjectsFromQuery(query)
	l1 = len(expected)
	l2 = len(subjects)

	if l1 != l2 || l1 != 0 && l2 != 0 {
		t.Errorf("expected %v != result %v by length %d != %d",
			expected,
			subjects,
			len(expected),
			len(subjects))
	} else if len(filters) != 0 {
		t.Errorf("expected %v must be not elastic", expected)
	}

	for i := range expectedFilters {
		if expectedFilters[i] != filters[i] {
			t.Errorf("expected filters %v != result filters %v by value:\n%s != %s",
				expectedFilters,
				filters,
				expectedFilters[i],
				filters[i])
		}
	}

	for i := range subjects {
		if expected[i] != subjects[i] {
			t.Errorf("expected %v != result %v by value:\n%s != %s",
				expected,
				subjects,
				expected[i],
				subjects[i])
		}
	}
}*/
