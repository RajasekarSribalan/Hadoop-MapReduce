

Work Hours Calculation Example:

In this example we are going to calculate the amount of time a user have spent on an activity, this is a simplified example of typical metering applications, say a user spend lots of time on different aspects of a website like facebook, and facebook wants to find where user spend moretime and where they dont spend time and have to properly allocate marketing budgets to address those areas where user activity is less or close feature where users don't show interest.

Typically a log of all users and all there activity would be available in their backend, here we see a simple example in which we don't tell what activity the user was engaged, but just put that they have started and stopeed the actitiy and we have the time when they started and when they stopped in the following manner


User Event Time
mmaniga,START,10
Ram,START,12
mmaniga,STOP,12
Dell,START,12
Dell,STOP,14
Ram,STOP,13


User Event Time
mmaniga,START,10
Ram,START,12
mmaniga,STOP,12
Dell,START,12
Dell,STOP,14
Ram,STOP,13

The task is to write a map reduce program to compute the total time spend by the user. In a real life scenario it woud be the total time user spend across many activiy. The output would be like this.

Dell	2
Ram	1
mmaniga	2


