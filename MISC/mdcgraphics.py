import turtle


class MDCGraphics:
    def __init__(self):
        self.wn = turtle.Screen()
        self.wn.title('MDC Graphics')
        self.t = turtle.Turtle()
        self.t.pencolor('blue')
        self.t.speed(0)

    def clear(self):
        self.t.clear()

    def reset(self):
        self.t.reset()

    def shape(self, size, side, angle):
        for i in range(side):
            self.t.forward(size)
            self.t.right(angle)

    def circle(self, radius, rng, pen_color='blue', angle=45):
        for i in range(rng):
            self.t.right(angle + 1)
            self.t.pencolor(pen_color)
            self.t.circle(radius + i)

    def mycircle(self):
        list_color = ['turquoise', '']#['green', 'yellow', 'red', 'green', 'yellow', 'red']#['green', 'yellow', 'red', 'orange', 'blue']
        radius = 10
        rang = 200
        for c in list_color:
            # rang = rang - 30
            self.circle(radius, rang, c)
        self.wn.exitonclick()


if __name__ == '__main__':
    g = MDCGraphics()
    g.mycircle()