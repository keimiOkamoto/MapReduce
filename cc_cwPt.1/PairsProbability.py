from mrjob,job import MRJob
import re
import heapq


#If there is a paragraph don't pair the word with the next one
#For the top ten  calculate which is the highest conditional probability for each one. 
#NOTE:  Conditional meaning that that the outcome depends on the previous event 

#1. Create accurate paris with punctuation formatting 
	#check for paragraph
	#-double for loop 

#2. Calculate the conditional probability .

#striped as it's using assosiative aray
class PairsProbability(MRJob) :

	next = False

	def mapper(self, _, line):
		global next

		if line != "":
			words  = list(re.findall(re.compile('\w+'), line))

			for word in range(len(words)):
				
				if next = True:l
					next = False
					yield words[word], 1

				if words[word] == "for":
					if word == len(range(len(words))):
						next = True

					if word + 1 !=  len(range(len(words))):
						yield words[word + 1], 1



	def combiner(self, key, values):
		yield (key, sum(values))

	def reducer(self, key, values):
		yield None, (sum(values), key)

	def topTen(self, _, results):
		print heapq.nlargest(10, results)

	def steps(self):
		return [
			self.mr(mapper = self.mapper,
				combiner = self.combiner,
				reducer = self.reducer),
			self.mr(reducer = self.topTen)
		]


if __name__ == '__main__':
	PairsProbability.run()

