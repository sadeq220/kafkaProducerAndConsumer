package ir.sadeqcloud.stream.utils;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * for injecting a bean in non-managed spring classes
 */
@Component("injectionUtil")
public class IoCContainerUtil implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    /**
     * after that last bean created this method will be called
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        IoCContainerUtil.applicationContext=applicationContext;
    }
    public static <T> T getBean(Class<T> tClass){
        return applicationContext.getBean(tClass);
    }
}
