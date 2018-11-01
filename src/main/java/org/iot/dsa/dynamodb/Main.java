package org.iot.dsa.dynamodb;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.historian.Historian;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.iot.dsa.dynamodb.db.DynamoDBProvider;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

/**
 * @author Daniel Shapiro
 */
public class Main extends Historian implements AWSCredentialsProvider {
	
	private static Main main;
	
//	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
	private final DynamoDBProvider provider;
	private Node rootNode;
	
	public static Main getInstance() {
		return main;
	}

	private Main() {
		if (main == null) {
			main = this;
		}
		this.provider = new DynamoDBProvider();
	}
	
	@Override
	public DatabaseProvider createProvider() {
		return provider;
	}
	
	@Override
	public void initialize(Node node) {
		this.rootNode = node;
		super.initialize(node);
		makeSetCredentialsAction(node);
	}
	
	void makeSetCredentialsAction(final Node node) {
		Action act = new Action(Permission.READ, new Handler<ActionResult>() {
			@Override
			public void handle(ActionResult event) {
				Node credNode = node.getChild(Util.CREDENTIALS, true);
				if (credNode == null) {
					credNode = node.createChild(Util.CREDENTIALS, true).setHidden(true).build();
				}
				Value idV = event.getParameter(Util.ACCESS_ID);
				Value secV = event.getParameter(Util.ACCESS_SECRET);
				if (idV != null && secV != null) {
					credNode.setRoConfig(Util.ACCESS_ID, idV);
					credNode.setPassword(secV.getString().toCharArray());
				} else {
					credNode.removeRoConfig(Util.ACCESS_ID);
				}
				initialize(node);
			}
		});
		Node credNode = node.getChild(Util.CREDENTIALS, true);
		Value idV = credNode == null ? null : node.getRoConfig(Util.ACCESS_ID);
		if (idV != null) {
			act.addParameter(new Parameter(Util.ACCESS_ID, ValueType.STRING, idV));
		} else {
			act.addParameter(new Parameter(Util.ACCESS_ID, ValueType.STRING));
		}
		act.addParameter(new Parameter(Util.ACCESS_SECRET , ValueType.STRING));
		Node anode = node.getChild(Util.ACT_SET_CREDENTIALS, true);
		if (anode == null) {
			node.createChild(Util.ACT_SET_CREDENTIALS, true).setAction(act).build().setSerializable(false);
		} else {
			anode.setAction(act);
		}
	}
	
	@Override
    public void stop() {
        super.stop();
        provider.stop();
    }
	
	public static void main(String[] args) {
        new Main().start(args);
    }

	@Override
	public AWSCredentials getCredentials() {
		Node credNode = rootNode.getChild(Util.CREDENTIALS, true);
		Value idV = credNode == null ? null : credNode.getRoConfig(Util.ACCESS_ID);
		char[] secretCharr = credNode == null ? null : credNode.getPassword();
		if (idV != null && secretCharr != null) {
			String id = idV.getString();
			String secret = String.valueOf(secretCharr);
			if (!id.isEmpty() && !secret.isEmpty()) {
				return new BasicAWSCredentials(id, secret);
			}
		}
		return DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
	}

	@Override
	public void refresh() {
		//no-op
	}
}
