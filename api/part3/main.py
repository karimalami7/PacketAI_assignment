#############################################
#
#   Part 3: A REST Api that 
#		
#		(i) returns the following words of a given word
#
#		(ii) selects randomly a word following a given word
#
#


from flask import Flask, request
from flask_restful import Resource, Api
from mongo_connector import MongoConnector
import sys


mongoConnector = None


class Ping(Resource):
    def get(self):  

    	return "Alive" 


class Next(Resource):
    def get(self):
    	global mongoConnector

    	word_document = mongoConnector.getDocumentByWord(request.args.get("word")[1:-1])
    	return word_document.possibleNextWord()
    	
    	
class Guess(Resource):
    def get(self):  
    	global mongoConnector

    	word_document = mongoConnector.getDocumentByWord(request.args.get("word")[1:-1])
    	return word_document.guessNextWord()  

class Ping(Resource):
    def get(self):  

    	return "Alive" 
	
if __name__ == "__main__":

	if len(sys.argv) != 5:
		print("Usage: problem3.py <host> <port> <database> <collection>")
		sys.exit(-1)

	mongoConnector = MongoConnector(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4])

	app = Flask(__name__)
	api = Api(app)

	api.add_resource(Ping, '/')
	api.add_resource(Next, '/gutenberg/predict/next/')
	api.add_resource(Guess, '/gutenberg/predict/random/')

	app.run(debug = True)