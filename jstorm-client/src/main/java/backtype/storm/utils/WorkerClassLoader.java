package backtype.storm.utils;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


public class WorkerClassLoader extends URLClassLoader {

	public static Logger LOG = Logger.getLogger(WorkerClassLoader.class);

	private ClassLoader parent;

	protected static WorkerClassLoader instance;

	protected static boolean enable;

	protected static Map<Thread, ClassLoader> threadContextCache;

	protected WorkerClassLoader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
		this.parent = parent;
		// TODO Auto-generated constructor stub
	}

	@Override
	public Class<?> loadClass(String name) throws ClassNotFoundException {
		Class<?> result = null;

		result = this.findLoadedClass(name);
		if (result != null) {
			return result;
		}

		try {
			result = parent.getParent().loadClass(name);
			if (result != null)
				return result;
		} catch (Exception e) {

		}
		
		try {
			if (name.startsWith("org.slf4j") == false
					&& name.startsWith("org.apache.log4j") == false
					&& name.startsWith("ch.qos.logback") == false) {
				result = findClass(name);

				if (result != null) {
					return result;
				}
			}

		} catch (Exception e) {

		}

		return parent.loadClass(name);
	}

	public static WorkerClassLoader mkInstance(URL[] urls, ClassLoader parent,
			boolean enable) {
		WorkerClassLoader.enable = enable;
		if (enable == false) {
			LOG.info("Don't enable UserDefine ClassLoader");
			return null;
		}

		synchronized (WorkerClassLoader.class) {
			if (instance == null) {
				instance = new WorkerClassLoader(urls, parent);

				threadContextCache = new ConcurrentHashMap<Thread, ClassLoader>();
			}

		}

		LOG.info("Successfully create classloader " + mk_list(urls));
		return instance;
	}

	public static WorkerClassLoader getInstance() {
		return instance;
	}

	public static boolean isEnable() {
		return enable;
	}

	public static void switchThreadContext() {
		if (enable == false) {
			return;
		}

		Thread thread = Thread.currentThread();
		ClassLoader oldClassLoader = thread.getContextClassLoader();
		threadContextCache.put(thread, oldClassLoader);
		thread.setContextClassLoader(instance);
	}

	public static void restoreThreadContext() {
		if (enable == false) {
			return;
		}

		Thread thread = Thread.currentThread();
		ClassLoader oldClassLoader = threadContextCache.get(thread);
		if (oldClassLoader != null) {
			thread.setContextClassLoader(oldClassLoader);
		} else {
			LOG.info("No context classloader of " + thread.getName());
		}
	}
	
	private static <V> List<V> mk_list(V... args) {
		ArrayList<V> rtn = new ArrayList<V>();
		for (V o : args) {
			rtn.add(o);
		}
		return rtn;
	}
}
