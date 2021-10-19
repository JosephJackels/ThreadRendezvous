package com.abc.handoff;

import com.abc.pp.stringhandoff.*;
import com.programix.thread.*;
/* Joseph Jackels
 * ICS 440 Final Project
 * Thread Rendezvous
 * 
 * General rules for how pass/receive work
 * 
 * Pass
 * 	1. Check for interrupts / shutdowns / if the passing slot has already been taken
 * 	2. Set flag that pass has been initiated
 * 	3. Set flag that pass is ready to begin handshake with receive
 * 	4. Wait until receive is ready **
 * 	5. Set message variable
 * 	6. Set flag that message variable has been set
 * 	7. Wait until message has been received **
 * 	8. Exit method, resetting object's variables
 * 
 * Receive
 * 	1. Check for interrupts / shutdowns / if the receiving slot has already been taken
 * 	2. Set flag that receive has been initiated
 * 	3. Set flag that receive is ready to begin handshake with pass
 * 	4. Wait until pass is ready **
 * 	5. Wait until message has been set **
 * 	6. Read message into local variable
 * 	7. Set flag that message has been received
 * 	8. Exit method, resetting object's variables
 * 
 * 	** ANY/ALL waiting is followed by a check for any interrupts/shutdowns that may have occurred while waiting
 * 
 * Timeout methods
 * 	The timeout given for pass/receive is assumed to be the maximum amount of time to wait for the handshake to initiate.
 * 	Later waits, i.e. when waiting for the message to be set/read, are assumed to be negligible in length because the handshake has begun and
 * 	these waits are only in place to ensure that the message variable is set before being read, and not cleared until after it is read.
 */
public class StringHandoffImpl implements StringHandoff {
	//message to be passed/received
	private String message;
	
	//booleans to keep track of if the StringHandoffImpl has already initiated its passer/receiver
	private boolean passInitiated;
	private boolean receiveInitiated;
	
	//booleans to track that the passer/receiver has passed its initial checks and is ready to interact with its counterpart
	private boolean passReady;
	private boolean receiveReady;
	
	//passer has set it's message, so receiver can read it safely now
	private boolean messageSet;
	//receiver has received the message, so passer can safely reset the message field and end itself
	private boolean messageReceived;
	
	//if this field is true, any passer or receiver should immediately end itself
	private boolean shutdown;
	
    public StringHandoffImpl() {
    	message = null;
    	passReady = false;
    	receiveReady = false;
    	messageSet = false;
    	messageReceived = false;
    	shutdown = false;
    	passInitiated = false;
    	receiveInitiated = false;
    }

    @Override
    public synchronized void pass(String msg, long msTimeout)
        throws InterruptedException, TimedOutException, ShutdownException, IllegalStateException {
    	//always check for interrupt and shutdown when beginning the call
    	interruptedCheck();
    	shutdownCheck();
    	
    	//check that another thread hasn't already claimed the 'passer' spot
    	if(passInitiated) {
    		throw new IllegalStateException("There is already a thread attempting a pass!");
    	}
    	try {
    		//claim passing spot for the object, let receiver know that a passer is ready to interact
    		passInitiated = true;
    		passReady = true;
    		if(receiveReady) {
    			//receiver is already ready! no need to worry about passer's timeout
    			//set message, let receiver know that message has been set
    			message = msg;
    			messageSet = true;
    			//handshake has begun, so reseting receiverReady prevents this passing thread from being
    			//interrupted and replaced with a different passing thread taking its place
    			receiveReady = false;
    			this.notifyAll();
    			while(!messageReceived) {
    				//wait until receiver has asserted that it has received the message, checking whether
    				//interrupts or shutdowns have been called while thread was waiting
    				this.wait();
    				interruptedCheck();
    				shutdownCheck();
    			}
    		} else if(msTimeout == 0L) {
    			//passer does not have a timeout, wait forever until receiver is ready
    			while(!receiveReady) {
    				//wait for receiver to be ready, checking for interrupts & shutdowns that could occur
    				//while waiting
	    			this.wait();
	    			interruptedCheck();
	    			shutdownCheck();
	    		}
    			//set message, let receiver know message has been set
    			message = msg;
    			messageSet = true;
    			//handshake has begun, so reseting receiverReady prevents this passing thread from being
    			//interrupted and replaced with a different passing thread taking its place
    			receiveReady = false;
    			this.notifyAll();
    			while(!messageReceived) {
    				//wait until receiver has read the message, checking interrupts/shutdowns whenever wait is ended
    				this.wait();
    				interruptedCheck();
    				shutdownCheck();
    			}
    		} else {
    			//has a timeout
    			//max time to wait for handshakewould be current time + desired timeout length
    			long endtime = System.currentTimeMillis() + msTimeout;
    			long msRemaining = msTimeout;
    			
    			//wait until receiver is ready or timeout is reached
    			while(!receiveReady && msRemaining >= 1L) {
    				//call wait for a maximum of msRemaining ms
    				//check interrupt/shutdown state upon returning from wait
    				this.wait(msRemaining);
    				interruptedCheck();
    				shutdownCheck();
    				
    				//recalculate msremaining based upon much time was already spent waiting
    				msRemaining = endtime - System.currentTimeMillis();
    			}
    			if(receiveReady) {
    				//exited wait because of a signal!
    				//msRemaining could also be 0l, but because the receiver accepted handshake in time we defer to
    				//continuing the pass
    				//set the message and notify receiver that message is ready to read
    				message = msg;
    				messageSet = true;
    				//handshake has begun, so reseting receiverReady prevents this passing thread from being
        			//interrupted and replaced with a different passing thread taking its place
        			receiveReady = false;
    				this.notifyAll();
    				while(!messageReceived) {
    					//wait until receiver has read the message
    					//check interrupt/shutdown state any time thread returns from waiting
    					this.wait();
    					interruptedCheck();
    					shutdownCheck();
    				}
    			} else {
    				//exited because timeout was reached!
    				throw new TimedOutException("Passer reached its timeout before receiver was ready!");
    			}
    		}
    	} finally {
    		//NO matter if hand off is completed, not completed, or partially completed, always reset
    		//any and all fields set by this method
    		message = null;
        	passReady = false;
        	messageSet = false;
        	messageReceived = false;
        	passInitiated = false;
    	}
    }

    @Override
    public synchronized void pass(String msg) throws InterruptedException, ShutdownException, IllegalStateException {
    	//always check states of interrupt/shutdown before calling pass method with a timeout of 0
    	interruptedCheck();
    	shutdownCheck();
    	pass(msg, 0L);
    }

    @Override
    public synchronized String receive(long msTimeout)
        throws InterruptedException, TimedOutException, ShutdownException, IllegalStateException {
    	//check for interrupts/shutdowns
    	interruptedCheck();
    	shutdownCheck();
    	
    	//only one thread can claim receiving spot at a time
    	if(receiveInitiated) {
    		throw new IllegalStateException("There is already a thread attempting to receive!");
    	}
    	
    	//holder for message that is passed to this thread
    	String passedMessage = null;
    	
    	try {
    		//claim receiving position, let passer know that receiver is ready
    		receiveInitiated = true;
    		receiveReady = true;
    		if(passReady) {    			
    			//passer is already ready! no need to worry about timeout!
    			//notify passer that receiver is ready, wait for passer to set message
    			this.notifyAll();
    			while(!messageSet) {    				
    				//check interrupt/shutdown state whenever returning from wait
    				this.wait();
    				interruptedCheck();
    				shutdownCheck();
    			}
    			//reset the passReady state so that the receiver can't be interrupted and replaced by another thread
    			passReady = false;
    			//copy the message into this threads local space, notify the passer that
    			//the message has been received allowing it to reset the message field and exit the method
    			passedMessage = message;
    			messageReceived = true;
    			this.notifyAll();
    		} else if(msTimeout == 0L) {
    			//receiver has no timeout! wait forever until passer is ready
    			while(!passReady) {
    				//wait until passer is ready, checking interrupt/shutdown states
    				this.wait();
    				interruptedCheck();
    				shutdownCheck();
    			}
    			//reset passReady state to prevent another thread from interrupting and claiming this receiving position
    			passReady = false;
    			//passer is ready!
    			//notify passer that it can set the message field
    			if(!messageSet) {
    				this.notifyAll();
    			}
    			while(!messageSet) {
    				//wait until message is set, checking for interrupts/shutdowns
    				this.wait();
    				interruptedCheck();
    				shutdownCheck();
    			}
    			//read message and notify passer that message has been read
    			passedMessage = message;
    			messageReceived = true;
    			this.notifyAll();
    		} else {
    			//receiver has a timeout!
    			//calculate maximum end time based on current time and timeout length
    			long endtime = System.currentTimeMillis() + msTimeout;
    			long msRemaining = msTimeout;
    			while(!passReady && msRemaining >= 1L) {
    				//wait a maximum of msRemaining ms, checking for interrupts/shutdowns
    				this.wait(msRemaining);
    				interruptedCheck();
    				shutdownCheck();
    				//recalculate remaining time
    				msRemaining = endtime - System.currentTimeMillis();
    			}
    			if(passReady) {
    				//exited wait because the passer is ready!
    				//reset passReady field to prevent this receiver from being replaced
    				passReady = false;
    				//tell passer it can set the message if it hasn't yet
    				if(!messageSet) {
    					this.notifyAll();
    				}
    				while(!messageSet) {
    					//wait for passer to set message, checking for interrupts/shutdowns
    					this.wait();
    					interruptedCheck();
    					shutdownCheck();
    				}
    				//read message and let passer know it has received the message
    				passedMessage = message;
    				messageReceived = true;
    				this.notifyAll();
    			} else {
    				throw new TimedOutException("Receiver reached its timeout before passer was ready!");
    			}
    		}
    		
    	} finally {
    		//reset fields set by this method.
    		//DO NOT reset messageReceived because the receiver will end before the passer has been
    		//notified that message has been received, passer will reset this field after it ends.
    		receiveReady = false;
    		receiveInitiated = false;
    	}
    	
        return passedMessage;
    }

    @Override
    public synchronized String receive() throws InterruptedException, ShutdownException, IllegalStateException {
    	//check for interrupts/shutdowns then call receive with 0L timeout
    	interruptedCheck();
    	shutdownCheck();
    	return receive(0L);
    }

    @Override
    public void shutdown() {
    	//set shutdown field so that any thread will know to shut itself down.
    	//this method DOES NOT shutdown the thread, the threads will shut themselves down upon calling shutdownCheck()
    	shutdown = true;
    }
    public void shutdownCheck() throws ShutdownException{
    	//shut this thread down, and notify all other threads to end their wait, where they will then check shutdown and end themselves accordingly
    	if(shutdown) {
    		this.notifyAll();
    		throw new ShutdownException("Shutdown has been called!");
    	}
    }
    public void interruptedCheck() throws InterruptedException{
    	//check for interruption, and notify all other threads to check themselves for interruption
    	if(Thread.currentThread().isInterrupted()) {
    		this.notifyAll();
    		throw new InterruptedException("Thread was interrupted!");
    	}
    }
    @Override
    public Object getLockObject() {
        return this;
    }
}