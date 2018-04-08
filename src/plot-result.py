import matplotlib.pyplot as plt
import math
import argparse

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Displays k-means results")
  parser.add_argument("classification_file", type=argparse.FileType('r'), help="The file containing classification results");
  parser.add_argument("centroid_file", type=argparse.FileType('r'), help="The file containing final centroids")
  parser.add_argument("--no-color", action="store_true", help="Do not color the data points (useful to show initialization)")

  args = parser.parse_args()
  color=[]
  x=[]
  y=[]
  area=[]
  for line in args.classification_file:
    c,xx,yy=str.split(line)
    if args.no_color:
      color.append("black")
    else:
      color.append(int(c))
    area.append(math.pi*6**2)
    x.append(xx)
    y.append(yy)

  plt.scatter(x,y,c=color,s=area)

  color=[]
  x=[]
  y=[]
  area=[]
  for line in args.centroid_file:
    xx,yy=str.split(line)
    color.append("red")
    area.append(300)
    x.append(xx)
    y.append(yy)

  plt.scatter(x,y,c=color,s=area)
  
  plt.show()
