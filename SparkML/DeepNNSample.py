import pandas 	as pd
import numpy	as np

# image tools
#from matplotlib import pyplot as plt
from PIL import Image

# fileystem tools
import os 
from glob import glob

#ML section
###########
import tensorflow as tf


from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets("mnist_data/", one_hot=True)

# defining graph
################
graph = tf.Graph()

with graph.as_default():
	batch_size 	= 128
	beta 		= 0.001
	image_size 	= 28
	num_labels 	= 10


	# input data. training data uses placeholders
	tf_train_dataset 	= tf.placeholder(tf.float32, shape=(batch_size, image_size*image_size))
	tf_train_labels		= tf.placeholder(tf.float32, shape=(batch_size, num_labels))
	tf_valid_dataset	= tf.constant(mnist.validation.images)
	tf_test_dataset		= tf.constant(mnist.test.images)

	# weights & bises for output/logit layer
	w_logit = tf.Variable(tf.truncated_normal([image_size * image_size, num_labels]))
	b_logit = tf.Variable(tf.zeros([num_labels]))


	# the model o = w*x + b
	def model(data):
		model_output = tf.matmul(data, w_logit) + b_logit
		return model_output


	# Training
	logits = model(tf_train_dataset)
	loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits, tf_train_labels))
	regularized_loss = tf.nn.l2_loss(w_logit)
	total_loss = loss + beta + regularized_loss

	#optimiser based on the cost function
	optimizer = tf.train.GradientDescentOptimizer(0.5).minimize(total_loss)

	#Predictions for the training, validation, & test data
	train_prediction 	= tf.nn.softmax(logits)
	valid_prediction 	= tf.nn.softmax(model(tf_valid_dataset))
	test_prediction 	= tf.nn.softmax(model(tf_test_dataset))

# define accuracy
def accuracy(predictions, labels):
	return (100.0 * np.sum(np.argmax(predictions,1) == np.argmax(labels,1)) / predictions.shape[0])


num_steps = 5001

with tf.Session(graph=graph) as session:
	tf.initialize_all_variables().run()

	print("Initialized")
	for step in range(num_steps):

		# generate a minibatch at each loop
		batch_data, batch_labels = mnist.train.next_batch(batch_size)

		# prepare the dictionary telling session where to find minibatch
		# key of dictionary is placeholder node of graph to be fed
		# value is the numpy array to feed into it
		feed_dict = {tf_train_dataset : batch_data, tf_train_labels : batch_labels}

		_, l, predictions = session.run([optimizer, loss, train_prediction], feed_dict=feed_dict)

		if(step % 500 == 0):
			print("Minibatch loss at step %d: %f" %(step, l))
			print("Minitab accuracy: %.1f%%" % accuracy(predictions, batch_labels))
			print("Validation accuracy: %.1f%%" % accuracy(valid_prediction.eval(), mnist.validation.labels))

	print("Test accuracy: %.1f%%" % accuracy(test_prediction.eval(), mnist.test.labels))







