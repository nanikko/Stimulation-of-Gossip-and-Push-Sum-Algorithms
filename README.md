Implemented a simulator to analyze convergence time of **Gossip algorithm** for group communication and **Push-Sum algorithm: Gossip-Based Computation of Aggregate Information** for sum computation on different network topologies like Full network, Line network, 2D Grid and Imperfect 2D Grid network. 

The push-sum algorithm was implemented as per specifications in the research paper "Gossip-Based Computation of Aggregate Information" available at [http://www.comp.nus.edu.sg/~ooibc/courses/cs6203/focs2003-gossip.pdf](http://www.comp.nus.edu.sg/~ooibc/courses/cs6203/focs2003-gossip.pdf)

For both the algorithms, graphs were plotted for Number of nodes vs Total time taken for convergence, and its was observed that in both the algorithms Full network worked best among all topologies and had quickest convergence time in order of O(log n) followed by Imperfect 2D Grid and then 2D Grid topology. The Line topology had worst convergence time of order of O(n-square)
