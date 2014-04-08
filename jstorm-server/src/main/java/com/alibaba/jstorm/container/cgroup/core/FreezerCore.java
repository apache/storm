package com.alibaba.jstorm.container.cgroup.core;

import java.io.IOException;

import com.alibaba.jstorm.container.CgroupUtils;
import com.alibaba.jstorm.container.Constants;
import com.alibaba.jstorm.container.SubSystemType;

public class FreezerCore implements CgroupCore {

	public static final String FREEZER_STATE = "/freezer.state";

	private final String dir;

	public FreezerCore(String dir) {
		this.dir = dir;
	}

	@Override
	public SubSystemType getType() {
		// TODO Auto-generated method stub
		return SubSystemType.freezer;
	}

	public void setState(State state) throws IOException {
		CgroupUtils.writeFileByLine(Constants.getDir(this.dir, FREEZER_STATE),
				state.name().toUpperCase());
	}

	public State getState() throws IOException {
		return State.getStateValue(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, FREEZER_STATE)).get(0));
	}

	public enum State {
		frozen, freezing, thawed;

		public static State getStateValue(String state) {
			if (state.equals("FROZEN"))
				return frozen;
			else if (state.equals("FREEZING"))
				return freezing;
			else if (state.equals("THAWED"))
				return thawed;
			else
				return null;
		}
	}

}
