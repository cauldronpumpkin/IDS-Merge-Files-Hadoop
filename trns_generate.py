import random
file = open("./trns1.txt","w+")
i = 1;
while i <= 100000000:
	file.write(str(i) + "," + str(random.randint(1,370104)) + "," + str(random.randint(10,10000)) + "\n")
	i += 1
file.close()

