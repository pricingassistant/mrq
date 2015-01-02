# Design

A talk with some slides about MRQ's design is upcoming.

A couple things to know:

- We use Redis as a main queue for task IDs
- We store metadata on the tasks in MongoDB so they can be browsable and managed more easily.
