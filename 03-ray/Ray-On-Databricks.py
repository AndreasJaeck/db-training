# Databricks notebook source
# MAGIC %md
# MAGIC ## Ray:  Reinforcement Learning and Distributed Python on Databricks
# MAGIC 
# MAGIC Ray is a fast and simple framework for building and running distributed applications.
# MAGIC 
# MAGIC It is packaged with the following libraries for accelerating machine learning and distributed workloads:
# MAGIC 
# MAGIC - Tune: Scalable Hyperparameter Tuning
# MAGIC - RLlib: Scalable Reinforcement Learning
# MAGIC - RaySGD: Distributed Training Wrappers
# MAGIC - RayServe: Scalable and Programmable Serving
# MAGIC 
# MAGIC It can be used to increase a cluster's capability by providing low-latency applications such as:
# MAGIC 
# MAGIC - High-performance computing
# MAGIC - Distributing functions that are not distributable by Spark
# MAGIC - Reinforcment learning
# MAGIC - Improving the performance of UDFs
# MAGIC - Simultations at scale
# MAGIC - Hyperparameter search
# MAGIC 
# MAGIC *All experiments were run on a cluster with 8 i3.xlarge instances on Databricks Runtime 8.4 ML*

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Init Script
# MAGIC 
# MAGIC If the cluster doesn't yet have Ray installed and running, run this script to create the init script. Configure the cluster to run the script created in the Advanced Options and restart the cluster.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC kernel_gateway_init = """
# MAGIC #!/bin/bash
# MAGIC 
# MAGIC #RAY PORT
# MAGIC RAY_PORT=9339
# MAGIC REDIS_PASS="d4t4bricks"
# MAGIC 
# MAGIC # install ray
# MAGIC /databricks/python/bin/pip install ray
# MAGIC 
# MAGIC # Install additional ray libraries
# MAGIC /databricks/python/bin/pip install ray[debug,dashboard,tune,rllib,serve]
# MAGIC 
# MAGIC # If starting on the Spark driver node, initialize the Ray head node
# MAGIC # If starting on the Spark worker node, connect to the head Ray node
# MAGIC if [ ! -z $DB_IS_DRIVER ] && [ $DB_IS_DRIVER = TRUE ] ; then
# MAGIC   echo "Starting the head node"
# MAGIC   ray start  --min-worker-port=20000 --max-worker-port=25000 --temp-dir="/tmp/ray" --head --port=$RAY_PORT --redis-password="$REDIS_PASS"  --include-dashboard=false
# MAGIC else
# MAGIC   sleep 40
# MAGIC   echo "Starting the non-head node - connecting to $DB_DRIVER_IP:$RAY_PORT"
# MAGIC   ray start  --min-worker-port=20000 --max-worker-port=25000 --temp-dir="/tmp/ray" --address="$DB_DRIVER_IP:$RAY_PORT" --redis-password="$REDIS_PASS"
# MAGIC fi
# MAGIC """ 
# MAGIC # Change ‘username’ to your Databricks username in DBFS
# MAGIC # Example: username = “stephen.offer@databricks.com”
# MAGIC # username = "<USERNAME>"
# MAGIC # dbutils.fs.put("dbfs:/Users/{0}/init/ray.sh".format(username), kernel_gateway_init, True)

# COMMAND ----------

username = "andreas.jack@databricks.com"
dbutils.fs.put("dbfs:/Users/{0}/init/ray.sh".format(username), kernel_gateway_init, True)

# COMMAND ----------

# Ray will crash unless sys.stdout is modified.
import sys

sys.stdout.fileno = lambda: 0

# COMMAND ----------

import ray

# Connect to the Ray clsuter
# _redis_password must be the same as specified in the init script.
ray.init(ignore_reinit_error=True, address='auto', _redis_password='d4t4bricks')

# COMMAND ----------

ray.cluster_resources()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Introduction to Ray
# MAGIC 
# MAGIC *Taken from https://docs.ray.io/en/latest/ray-overview/index.html*

# COMMAND ----------

# First, decorate your function with @ray.remote to declare that you want to run this function remotely. 
# Lastly, call that function with .remote() instead of calling it normally. 
# This remote call yields a future, or ObjectRef that you can then fetch with ray.get.

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(4)]
print(ray.get(futures)) # [0, 1, 4, 9]

# COMMAND ----------

# Ray provides actors to allow you to parallelize an instance of a class in Python. 
# When you instantiate a class that is a Ray actor, Ray will start a remote instance of that class in the cluster. 
# This actor can then execute remote method calls and maintain its own internal state.

@ray.remote
class Counter(object):
    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1

    def read(self):
        return self.n

counters = [Counter.remote() for i in range(4)]
[c.increment.remote() for c in counters]
futures = [c.read.remote() for c in counters]
print(ray.get(futures)) # [1, 1, 1, 1]

# COMMAND ----------

from ray import tune
# Tune is a library for hyperparameter tuning at any scale. 
# With Tune, you can launch a multi-node distributed hyperparameter sweep in less than 10 lines of code. 
# Tune supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

def objective(step, alpha, beta):
    return (0.1 + alpha * step / 100)**(-1) + beta * 0.1

  
def training_function(config):
    # Hyperparameters
    alpha, beta = config["alpha"], config["beta"]
    for step in range(10):
        # Iterative training function - can be any arbitrary training procedure.
        intermediate_score = objective(step, alpha, beta)
        # Feed the score back back to Tune.
        tune.report(mean_loss=intermediate_score)


analysis = tune.run(
    training_function,
    config={
        "alpha": tune.grid_search([0.001, 0.01, 0.1]),
        "beta": tune.choice([1, 2, 3])
    })

print("Best config: ", analysis.get_best_config(
    metric="mean_loss", mode="min"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simple Distributed Python example
# MAGIC 
# MAGIC Let's compare the difference between a distributed and non-distributed function on a mock slow function:

# COMMAND ----------

import time

def slow_f(x):
    time.sleep(1)
    return x * x

@ray.remote
def f(x):
    time.sleep(1)
    return x * x

n_iters = 10
start_time = time.time()
for i in range(n_iters):
    slow_f(i)
elapsed = time.time() - start_time
print("Non-distributed function takes {} (s)".format(elapsed))
  
start_time = time.time()
res = ray.get([f.remote(i) for i in range(n_iters)])
elapsed = time.time() - start_time
print("Distributed function takes {} (s)".format(elapsed))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Increasing the performance of Python UDFs
# MAGIC 
# MAGIC Custom functions that are slow and can't be distributed by Spark can be distributed using Ray. In this example, we'll create a function that predicts the next day of COVID deaths per county in the US by a partition of the data for every day. 

# COMMAND ----------

# Get COVID-19 data
covid_df = (spark
            .read
            .option("header", "true") 
            .option('inferSchema', 'true')
            .csv('/databricks-datasets/COVID/USAFacts/covid_deaths_usafacts.csv'))

# COMMAND ----------

display(covid_df)

# COMMAND ----------

from pyspark.sql.functions import *

select_cols = covid_df.columns[4:]

df = (covid_df
     .select(
       col('County Name').alias('county_name'),
       array([col(n) for n in select_cols]
       ).alias('deaths')))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's define a UDF that does not use Ray to distribute the UDF. Note that these functions are not production quality predictions, merely illustrations of distributed vs non-distributed.
# MAGIC 
# MAGIC This should take around 5 minutes to complete.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn import linear_model

@pandas_udf(ArrayType(LongType()))
def non_ray_udf(s):
    s = list(s)
    pred = []
    for i in range(len(s)):
        x = list(range(i+1))
        x = np.asarray([[n] for n in x])
        y = s[:i+1]
        y = np.asarray(y)
        
        reg = linear_model.ElasticNet().fit(x, y)
 
        p = reg.predict(np.array([[i + 1]]))
        pred.append(p[0])

    return pd.Series(pred)

res = df.select("county_name", "deaths", non_ray_udf("deaths").alias("preds"))
display(res)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we can use a Ray remote function to distribute the multiple model training calls.
# MAGIC 
# MAGIC This will take around 2.5 minutes to run instead of 5 minutes.

# COMMAND ----------

@ray.remote
def linear_pred(x,y, i):
    reg = linear_model.ElasticNet().fit(x, y)
    p = reg.predict(np.array([[i + 1]]))
    return p[0]
  

@pandas_udf(ArrayType(LongType()))
def ray_udf(s):
    s = list(s)

    pred = []
    workers = []
    for i in range(len(s)):
        x = list(range(i+1))
        x = np.asarray([[n] for n in x])
        y = s[:i+1]
        y = np.asarray(y)

        workers.append(linear_pred.remote(x, y, i))

    pred = ray.get(workers)
    return pd.Series(pred)

res = df.select("county_name", "deaths", ray_udf("deaths").alias("preds"))
display(res)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reinforcement Learning
# MAGIC 
# MAGIC RLlib is "an open-source library for reinforcement learning that offers both high scalability and a unified API for a variety of applications". In this example, RLlib will train an IMPALA (Importance Weighted Actor-Learner Architecture) agent on an OpenAI Gym Pong environment.

# COMMAND ----------

from ray import tune

tune.run(
    "IMPALA",
    stop={"training_iteration": 20},
    config={
        "env": "PongNoFrameskip-v4",
        "num_gpus": 0,
        "rollout_fragment_length": 50,
        "train_batch_size": 500,
        "num_workers": 7,
        "num_envs_per_worker": 1,
    },
)

# COMMAND ----------

tune.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train AlphaZero
# MAGIC 
# MAGIC The IMPALA example has only a few configurations, to showcase how quickly one can create very complicated algorithms with many parameters, let's use one of the most cutting-edge algorithms.
# MAGIC 
# MAGIC AlphaZero was made by Google DeepMind and this algorithm was used to beat the regining champion computer engines in Chess, Go, and Shogi. It's overkill for a simple CartPole environment, but illustrative of how to utilize these newer technolgoies in only a few lines of code.

# COMMAND ----------

"""Example of using training on CartPole."""
import ray
from ray import tune
from ray.rllib.contrib.alpha_zero.models.custom_torch_models import DenseModel
from ray.rllib.contrib.alpha_zero.environments.cartpole import CartPole
from ray.rllib.models.catalog import ModelCatalog


def trainAlphaZero():
    ModelCatalog.register_custom_model("dense_model", DenseModel)

    tune.run(
        "contrib/AlphaZero",
        stop={"training_iteration": 20},
        max_failures=0,
        config={
            "env": CartPole,
            "num_workers": 7,
            "rollout_fragment_length": 50,
            "train_batch_size": 500,
            "sgd_minibatch_size": 64,
            "lr": 1e-4,
            "num_sgd_iter": 1,
            "mcts_config": {
                "puct_coefficient": 1.5,
                "num_simulations": 10,
                "temperature": 1.0,
                "dirichlet_epsilon": 0.20,
                "dirichlet_noise": 0.03,
                "argmax_tree_policy": False,
                "add_dirichlet_noise": True,
            },
            "ranked_rewards": {
                "enable": True,
            },
            "model": {
                "custom_model": "dense_model",
            },
        },
    )

trainAlphaZero()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Custom Environments
# MAGIC 
# MAGIC Customer use cases will not be simplistic cartpole environments, so let's create a customized environment to showcase how to create new applications.

# COMMAND ----------

import gym
from gym.spaces import Discrete, Box
from ray import tune

class SimpleCorridor(gym.Env):
  
    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, self.end_pos, shape=(1, ))
        
    def reset(self):
        self.cur_pos = 0
        return [self.cur_pos]
      
    def step(self, action):
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        elif action == 1:
            self.cur_pos += 1
        done = self.cur_pos >= self.end_pos
        return [self.cur_pos], 1 if done else 0, done, {}
      
tune.run(
    "PPO",
    stop={"training_iteration": 20},
    config={
        "env": SimpleCorridor,
        "num_workers": 16,
        "env_config": {"corridor_length": 3}}
)   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Additional Resources:
# MAGIC 
# MAGIC - Ray website: https://ray.io/
# MAGIC - Ray documentation: https://docs.ray.io/en/latest/
