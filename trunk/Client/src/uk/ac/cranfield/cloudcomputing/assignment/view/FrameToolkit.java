package uk.ac.cranfield.cloudcomputing.assignment.view;

import java.awt.Dimension;
import java.awt.Image;
import java.awt.Toolkit;

import javax.swing.JPanel;


/**
 * The class provides some useful information about screen/panel size.
 * It is also capable of creating Images objects.
 * @author Mateusz Golab
 * @version 1.0
 */
public class FrameToolkit
{
    
    /**
     * Static instance of default java toolkit.
     */
    private static Toolkit toolkit = Toolkit.getDefaultToolkit();
    
    /**
     * Returns size of screen for starting application as Dimension object.
     * @return screen size
     */
    public static Dimension getSize()
    {
        return new Dimension(toolkit.getScreenSize().width / 2, toolkit.getScreenSize().height / 2);
    }
    
    /**
     * Returns screen width.
     * @return screen width
     */
    public static int getMaxWidth()
    {
        return toolkit.getScreenSize().width;
    }
    
    /**
     * Returns screen height.
     * @return screen height
     */
    public static int getMaxHeight()
    {
        return toolkit.getScreenSize().height;
    }
    
    /**
     * Returns the center of given panel as Dimension object.
     * @param panel panel which center is returned
     * @return center of panel as Dimension
     */
    public static Dimension getPanelCenter(JPanel panel)
    {
        return new Dimension(panel.getWidth() / 2, panel.getHeight() / 2);
    }
    
    /**
     * Creates Image object from file's path given as parameter.
     * @param path path of the image file.
     * @return Image object
     */
    public static Image getImage(String path)
    {
        return toolkit.getImage(path);
    }
    
}
