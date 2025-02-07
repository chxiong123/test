import sys
import pandas as pd



def dev(x):
      """
      该函数是在dev分支上开发的
      """
      print(x)

def test2(x):
      print(x)

def feature1():
      """
      该函数是在feature分支上开发的
      """
      print('feature1')
      print('feature2')
      print('feature3')

if __name__ == '__main__':

      test2('我要学习AI2')

      dev('我要学习AI')

      feature1()

